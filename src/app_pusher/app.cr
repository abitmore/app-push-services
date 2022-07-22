require "log"
require "json"

# 用户：提醒管理配置数据
# {
#   id:      "7.0.xx",
#   account: "1.2.#{target_account_id}",
#   catalog: "bcdss.reminder.settings.v1",
#   key:     "app",
#   value:   {
#     "@provider": "xxx bot",
#     "@lang":     "en",
#     feature1:    true,
#     feature2:    false,
#   },
# }
#
# 用户：绑定的社交媒体账号数据
# {
#   id:      "7.0.xx",
#   account: "1.2.#{target_account_id}",
#   catalog: "bcdss.socialmedia.v1",
#   key:     "telegram",
#   value:   "usename",
# }
#
# 系统：已认证的社交媒体账号信息
# {
#   id:      "7.0.xx",
#   account: "1.2.#{verifier_account_id}",
#   catalog: "bcdss.socialmedia.verified.v1.#{target_account_id}",
#   key:     "dynamic_verify_key",
#   value:   {
#     account:     "target_account_id",
#     socialmedia: "telegram",
#     username:    "username",
#     cid:         "chat_id",
#   },
# }

module AppPusher
  class App
    Log = ::Log.for("app")

    UserStorageCatalog_SocialMedia              = "bcdss.socialmedia.v1"                              # => catalog: 用户绑定的社交媒体信息
    UserStorageKey_SocialMedia_telegram         = "telegram"                                          # => key: 社交媒体：账号对应的存储 KEY
    AppStorageCatalog_SocialMediaVerifiedPrefix = "bcdss.socialmedia.verified.v1"                     # => catalog: 社交媒体账号认证的 catalog 前缀
    AppStorageCatalog_SocialMediaVerified       = "#{AppStorageCatalog_SocialMediaVerifiedPrefix}.%s" # => catalog: 官方对用户绑定的社交媒体进行认证的信息存储位置
    UserStorageCatalog_ReminderSettings         = "bcdss.reminder.settings.v1"                        # => catalog: 用户当前提醒配置
    UserStorageKey_ReminderSettings             = "app"                                               # => key: 提醒配置的KEY 固定值
    SpecSettingKey_Provider                     = "@provider"                                         # => 提醒设置特殊的KEY：推送来源
    SpecSettingKey_Language                     = "@lang"                                             # => 提醒设置特殊的KEY：多语言（用于确定push时候的语言）
    AppCatalog_AppSettings                      = "app.settings"                                      # => APP链上动态配置信息 catalog
    AppKey_AppSettingsCommonVerLatest           = "common.settings.ver.latest"                        # => APP链上动态配置信息 通用配置KEY的最新指向

    # => P2P抵押贷借款对象到期提醒间隔
    CreditDealRepayReminderInterval = {
      3600_u32 * 24 * 3, # => 3天
      3600_u32 * 24,     # => 1天
      3600_u32 * 12,     # => 12小时
      3600_u32 * 4,      # => 4小时
      3600_u32 * 1,      # => 1小时
    }

    # => 各种URL配置
    struct SURLConfig
      getter url_explorer_trx : String?
      getter url_explorer_block : String?
      getter url_explorer_account : String?
      getter url_explorer_asset : String?
      getter url_explorer_oid : String?

      def initialize(@url_explorer_trx = nil, @url_explorer_block = nil, @url_explorer_account = nil, @url_explorer_asset = nil, @url_explorer_oid = nil)
      end
    end

    # => 提醒配置数据结构
    struct SReminderSettings
      getter provider : String
      getter lang : String

      @features : Hash(String, Bool)

      def initialize(@provider, @lang)
        @features = typeof(@features).new
      end

      def is_feature_enabled?(feature_key) : Bool
        !!@features[feature_key]?
      end

      def set_feature_flag(feature_key : String, value : Bool)
        @features[feature_key] = value
      end
    end

    # => 认证数据结构
    struct SSocialMediaVerified
      getter account_id : String
      getter socialmedia : String
      getter username : String
      getter chat_id : String

      def initialize(@account_id, @socialmedia, @username, @chat_id)
      end

      def initialize(value : JSON::Any)
        @account_id = value["account"].as_s
        @socialmedia = value["socialmedia"].as_s
        @username = value["username"].as_s
        @chat_id = value["cid"].as_s
      end
    end

    # => 用户连接的社交媒体数据结构
    struct SSocialMediaConnection
      getter verify_key : String
      getter username : String
      getter is_verified : Bool
      getter chat_id : String?

      def initialize(@verify_key, @username, @is_verified, @chat_id : String? = nil)
      end
    end

    # => P2P抵押贷借款信息，会用处理临近还款日期提醒。
    struct SRuntimeCreditDealObject
      getter borrower : String              # => 借款人
      getter debt_asset : String            # => 借款资产
      getter debt_amount : Int64            # => 剩余借款
      getter latest_repay_time : UInt32     # => 还款期限
      property triggered_index : Int32 = -1 # => 已经触发的位置。默认 -1。

      def is_fully_triggered?
        return @triggered_index + 1 >= CreditDealRepayReminderInterval.size
      end

      def is_deal_expired?(block_timestamp)
        return block_timestamp >= @latest_repay_time
      end

      def initialize(@borrower, @debt_asset, @debt_amount, @latest_repay_time, init_block_time : UInt32)
        if tri_idx = calc_trigger_index(init_block_time)
          @triggered_index = tri_idx
        end
      end

      def calc_trigger_index(now_ts : UInt32) : Int32?
        result : Int32? = nil

        CreditDealRepayReminderInterval.size.times do |i|
          check_index = @triggered_index + i + 1

          second = CreditDealRepayReminderInterval[check_index]?
          break if second.nil?

          if now_ts + second >= @latest_repay_time
            result = check_index
          else
            break
          end
        end

        return result
      end
    end

    getter config : Config                    # => 配置信息
    getter push_service : TelegramPushService # => 电报推送服务
    getter app_start_time : Int64             # => 启动时间戳
    getter tick_n : UInt64 = 0_u64            # => tick 的区块数量

    @all_reminder_settings = Hash(String, SReminderSettings).new                 # => 所有提醒配置   KEY: account_id   VALUE: 配置信息
    @all_verified_hash = Hash(String, SSocialMediaVerified).new                  # => 所有认证信息   KEY: verify_key   VALUE: 认证信息
    @all_connection_hash = Hash(String, SSocialMediaConnection).new              # => 所有绑定信息   KEY: account_id   VALUE: 连接信息
    @reminder_feature_default_value = Hash(String, Bool).new                     # => 提醒配置项目的默认值 KEY: feature_key VALUE: 默认值
    @push_mark_fill_order = Hash(String, Bool).new                               # => 推送标记: 同一个 order id 在同一个区块里的 fill_order 不重复推送 KEY: order_id
    @monitor_all_credit_deal_object = Hash(String, SRuntimeCreditDealObject).new # => 监控：所有P2P借贷对象 KEY: deal id
    @cache_current_block_txid = Hash(UInt16, String).new                         # => 缓存：当前最新区块的 txid，下个区块会清除。
    @trace_deleted_ids = Set(String).new                                         # => 追踪需要删除的对象 由于订阅大部分API不支持对象删除，故定期查询。

    # 获取 client 实例
    getter(client : BitShares::Client) { BitShares::Client.new(BitShares::Config.new.tap { |cfg| cfg.api_nodes = @config.api_node }) }

    # 获取 URL 配置信息
    getter(url_config : SURLConfig?) {
      result = case client.graphene_chain_id
               when "4018d7844c78f6a6c41c6a552b898022310fc5dec06da467ee7905a8dad512c8" # => BTS MAINNET
                 # => TODO: bts.ai 宕机中
                 # SURLConfig.new(
                 #   url_explorer_trx: "https://bts.ai/tx/%s",
                 #   url_explorer_block: "https://bts.ai/block/%s",
                 #   url_explorer_account: "https://bts.ai/u/%s",
                 #   url_explorer_asset: "https://bts.ai/a/%s",
                 #   url_explorer_oid: "https://bts.ai/oid?oid=%s",
                 # )
                 SURLConfig.new(
                   url_explorer_trx: "https://blocksights.info/#/txs/%s",
                   url_explorer_block: "https://blocksights.info/#/blocks/%s",
                   url_explorer_account: "https://blocksights.info/#/accounts/%s",
                   url_explorer_asset: "https://blocksights.info/#/assets/%s",
                   url_explorer_oid: "https://blocksights.info/#/objects/%s",
                 )
               when "39f5e2ede1f8bc1a3a54a7914414e3779e33193f1f5693510e73cb7a87617447" # => BTS TESTNET
                 # => TODO: 需要在浏览器手动切换下网络 到 testnet
                 SURLConfig.new(
                   url_explorer_trx: "https://blocksights.info/#/txs/%s",
                   url_explorer_block: "https://blocksights.info/#/blocks/%s",
                   url_explorer_account: "https://blocksights.info/#/accounts/%s",
                   url_explorer_asset: "https://blocksights.info/#/assets/%s",
                   url_explorer_oid: "https://blocksights.info/#/objects/%s",
                 )
               when "cd931cb96d657ff0ef0226f7ae9d25175b3cc96a84490a674ed36170830324e7" # => NBS MAINNET
                 SURLConfig.new(
                   url_explorer_trx: "https://explorer.nbs.plus/tx/%s",
                   url_explorer_block: "https://explorer.nbs.plus/block/%s",
                   url_explorer_account: "https://explorer.nbs.plus/u/%s",
                   url_explorer_asset: "https://explorer.nbs.plus/a/%s",
                   url_explorer_oid: "https://explorer.nbs.plus/oid?oid=%s",
                 )
               else
                 nil
               end
      result
    }

    # 对象订阅
    getter(subscriber : ObjectSubscriber) { ObjectSubscriber.new(client) }

    def initialize(@config)
      @push_service = TelegramPushService.new(@config.telegram_bot_token)
      @app_start_time = Time.utc.to_unix
    end

    # 运维消息推送：普通信息
    private def push_to_administrator(message : String, format : TelegramPushService::TextFormat = :text)
      if chat_id = @config.telegram_private_chat_id
        @push_service.add_task(chat_id, message, format)
      end
    end

    # 运维消息推送：代码异常信息
    private def push_to_administrator(e : Exception, additional_data = nil)
      if chat_id = @config.telegram_private_chat_id
        @push_service.add_task(chat_id, e, additional_data)
      end
    end

    # => TODO: 根据需求优化 如果大批量推送后续考虑并行化处理
    def push_to_user(title : String, msg_or_msgarray : String | Array(String), target_chat_id : String | Array(String), format : TelegramPushService::TextFormat = :html)
      final_msg = String.build do |io|
        io << title
        io << "\n"
        if msg_or_msgarray.is_a?(String)
          io << msg_or_msgarray.as(String)
        else
          msg_or_msgarray.each_with_index { |msg, idx|
            io << msg
            io << "\n" if idx + 1 != msg_or_msgarray.size
          }
        end
      end

      @push_service.add_task(target_chat_id, final_msg, format)
    end

    # 启动
    def run
      loop do
        begin
          main_loop
          break
        rescue e : Exception
          on_exception(e)
        end
        sleep(1)
      end
    end

    private def check_get_block_operation_history_api
      begin
        client.call_history("get_block_operation_history", [1])
      rescue e : BitShares::ResponseError
        return false if e.graphene_error_message =~ /Method not found/i
      end
      return true
    end

    # => 计划任务: 加载所有提醒设置默认值
    private def load_reminder_default_value
      @reminder_feature_default_value.clear

      # => APP链上设置账号未配置
      app_settings_account_id = @config.app_settings_account_id
      return if app_settings_account_id.nil? || app_settings_account_id.empty?

      data_array = query_storage(app_settings_account_id, AppCatalog_AppSettings)

      # => latest key 不存在
      latest_common_ver_item = data_array.find { |storage_item| storage_item["key"].as_s == AppKey_AppSettingsCommonVerLatest }
      return if latest_common_ver_item.nil?
      latest_common_ver = latest_common_ver_item["value"].as_s

      # => common ver 设置项不存在
      common_ver_item = data_array.find { |storage_item| storage_item["key"].as_s == latest_common_ver }
      return if common_ver_item.nil?

      # => 获取所有 提醒配置 features 数组
      features_groups = common_ver_item.dig?("value", "reminder_data", "features").try &.as_a
      return if features_groups.nil?

      # => 加载所有 feature key 以及默认值
      features_groups.each { |group|
        group.as_a.each { |item|
          @reminder_feature_default_value[item["key"].as_s] = if default_item = item["default"]?
                                                                default_item.is_true?
                                                              else
                                                                false
                                                              end
        }
      }
    end

    # => 计划任务：定期删除链上已经不存在的对象。
    private def trace_deleted_objects
      removed_ids = [] of String

      list = @trace_deleted_ids.to_a
      client.query_objects(list).tap { |result| list.each { |oid| removed_ids << oid if !result.has_key?(oid) } }

      # => 清理追踪列表和缓存
      if !removed_ids.empty?
        Log.debug { "clean removed objects: #{removed_ids}" }
        removed_ids.each do |oid|
          @trace_deleted_ids.delete(oid)
          subscriber.delete_object(oid)
        end
      end
    end

    private def format_running_time
      diff_seconds = Time.utc.to_unix - @app_start_time

      days = diff_seconds // 86400
      hours = diff_seconds % 86400 // 3600
      min = diff_seconds % 86400 % 3600 // 60
      sec = diff_seconds % 60

      if days > 0
        return "#{days} days #{sprintf("%02d:%02d:%02d", hours, min, sec)}"
      else
        return sprintf("%02d:%02d:%02d", hours, min, sec)
      end
    end

    # => 日志：记录一些主循环日志，这些日志不会记录到文件中。
    private def log_for_main
      if @config.log_to_file
        Log.trace { yield }
      else
        Log.info { yield }
      end
    end

    # => 计划任务：定期输出运行状态信息 --debug 参数才会显示，默认日志不显示 trace level。
    private def print_app_status
      log_for_main { "-----------------------------------------status---------------------------------------------" }

      s = @push_service.statistics
      n_verified = 0
      @all_connection_hash.each_value { |v| n_verified += 1 if v.is_verified }

      log_for_main { sprintf("%-45s%-45s", "telegram cumulatively sent: #{s.success}", "failed: #{s.failed}") }
      log_for_main { sprintf("%-45s%-45s", "all social media: #{@all_connection_hash.size}", "all verified: #{n_verified}") }
      log_for_main { "enabled reminder items: #{@all_reminder_settings.size}" }
      log_for_main { "local credit deal objects: #{@monitor_all_credit_deal_object.size}" }
      log_for_main { "supported features: #{@reminder_feature_default_value.size}" }

      log_for_main { "running time: #{format_running_time} ##{@tick_n}" }
      log_for_main { "ver: #{AppPusher::VERSION}" }

      log_for_main { "--------------------------------------------------------------------------------------------" }
    end

    # 绑定间隔调用，在指定时间内重复调用会被忽略。
    private def binding_limit_call(interval_seconds, init_ts = 0_i64, &block)
      last_ts = init_ts
      return ->(now_ts : Int64) {
        if now_ts >= last_ts + interval_seconds
          block.call

          last_ts = now_ts
        end
      }
    end

    # 主循环
    private def main_loop
      # => 检测API节点状况
      if !check_get_block_operation_history_api
        Log.error { "API node is missing get_block_operation_history API." }
        exit
      end

      # => 绑定计划任务
      start_ts = Time.utc.to_unix
      limit_load_reminder_default_value = binding_limit_call(600, &->load_reminder_default_value)
      limit_print_app_status = binding_limit_call(60, start_ts, &->print_app_status)
      limit_trace_deleted_objects = binding_limit_call(300, start_ts, &->trace_deleted_objects)

      # => call
      limit_load_reminder_default_value.call(start_ts)

      # => 初始化所有数据
      init_block_time = BitShares::Utility.parse_time_string_i64(client.call_db("get_dynamic_global_properties")["time"].as_s)
      init_data_once
      init_all_credit_deal_object_once(init_block_time.to_u32)

      # => 初始化订阅
      subscriber.start

      # => 订阅新区块
      client.loop_new_block(0.2) do |new_block_number|
        raise BitShares::SocketClosed.new("set_subscribe_callback trigger websocket closed.") if subscriber.disconnected

        @tick_n += 1

        log_for_main { "new block: #{new_block_number}" }

        # => 新的区块，清理相关数据。
        @push_mark_fill_order.clear
        @cache_current_block_txid.clear

        # => 获取新区块数据
        json_block = client.call_db("get_block", [new_block_number])
        json_operation_history_array = client.call_history("get_block_operation_history", [new_block_number]).as_a
        block_timestamp = BitShares::Utility.parse_time_string_i64(json_block["timestamp"].as_s).to_u32

        # => 所有当前区块的所有 op 数据 REMARK: 低概率数据不全，单个区块单个用户 op 太多，被 remove 了。
        scan_operation_historys(json_block, json_operation_history_array, new_block_number, block_timestamp)

        # => 处理所有P2P借贷提醒
        process_all_credit_deal_object(block_timestamp)

        # => 处理计划任务，这里放在 op 扫描完毕之后。
        now_ts = Time.utc.to_unix
        limit_load_reminder_default_value.call(now_ts)
        limit_print_app_status.call(now_ts)
        limit_trace_deleted_objects.call(now_ts)
      end
    end

    # => 处理所有 deal object
    private def process_all_credit_deal_object(block_timestamp)
      @monitor_all_credit_deal_object.keys.each do |deal_id|
        deal_object = @monitor_all_credit_deal_object[deal_id]

        if deal_object.is_fully_triggered? || deal_object.is_deal_expired?(block_timestamp)
          @monitor_all_credit_deal_object.delete(deal_id) # => 删除
        elsif tri_idx = deal_object.calc_trigger_index(block_timestamp)
          # => 更新数据
          deal_object.triggered_index = tri_idx
          if deal_object.is_fully_triggered?
            @monitor_all_credit_deal_object.delete(deal_id) # => 删除
          else
            @monitor_all_credit_deal_object[deal_id] = deal_object # => 更新
          end

          # => 处理提醒
          handle_credit_deal_repayment_date_reminder(deal_id, deal_object, CreditDealRepayReminderInterval[tri_idx])
        end
      end
    end

    private def calc_transaction_id(transactions, trx_in_block : UInt16)
      if txid = @cache_current_block_txid[trx_in_block]?
        return txid
      else
        tx_json = transactions[trx_in_block]

        tx = Graphene::Serialize::Pack(Graphene::Operations::T_transaction).from_graphene_json(tx_json, client.graphene_address_prefix)
        txid = BitShares::Utility.sha256(tx.pack)[0, 20]

        result = txid.hexstring
        @cache_current_block_txid[trx_in_block] = result
        return result
      end
    end

    private def scan_operation_historys(json_block, data_array, block_num, block_timestamp)
      transactions = json_block["transactions"]

      # using operation_result = fc::static_variant <
      # /* 0 */ void_result,
      # /* 1 */ object_id_type,
      # /* 2 */ asset,
      # /* 3 */ generic_operation_result,
      # /* 4 */ generic_exchange_operation_result,
      # /* 5 */ extendable_operation_result
      # >
      data_array.each do |item|
        # {"id" => "1.11.1210661811",
        #  "op" =>
        #   [19, {opdata}],
        #  "result" => [0, {}],
        #  "block_num" => 70523179,
        #  "trx_in_block" => 0,
        #  "op_in_trx" => 0,
        #  "virtual_op" => 0}
        handle_opdata(transactions, item, item["op"].as_a, item["result"], block_num, block_timestamp)
      end
    end

    def try_push?(target_account_id, feature_key : String)
      # => 尚未连接社交媒体
      connection_data = @all_connection_hash[target_account_id]?
      if connection_data.nil?
        # Log.debug { "user #{account_id_to_name(target_account_id)} has not yet connected to the telegram." }
        return
      end

      # => 社交媒体未经过认证
      if !connection_data.is_verified
        Log.debug { "user #{account_id_to_name(target_account_id)}'s telegram account is not verified." }
        return
      end

      # => 提醒未配置
      reminder_item = @all_reminder_settings[target_account_id]?
      if reminder_item.nil?
        Log.debug { "user #{account_id_to_name(target_account_id)} has not enabled reminders." }
        return
      end

      # => 由其他推送机器人处理
      if reminder_item.provider != @push_service.bot_username
        Log.debug { "reminders for #{account_id_to_name(target_account_id)} are handled by the #{reminder_item.provider} bot." }
        return
      end

      # => 通知未开启
      if !reminder_item.is_feature_enabled?(feature_key)
        Log.debug { "user #{account_id_to_name(target_account_id)} has not enabled the reminder of the #{feature_key} feature." }
        return
      end

      # => 允许发送通知给指定 chat_id
      yield connection_data.chat_id.not_nil!, reminder_item.lang
    end

    private def handle_opdata(transactions, hist, op, result, block_num, block_timestamp)
      opcode = op.first.as_i.to_i8
      opdata = op.last
      case opcode
      when BitShares::Blockchain::Operations::Transfer.value
        on_op_transfer(transactions, hist, opdata, result, block_num, block_timestamp)
      when BitShares::Blockchain::Operations::Fill_order.value
        on_op_fill_order(transactions, hist, opdata, result, block_num, block_timestamp)
      when BitShares::Blockchain::Operations::Proposal_create.value
        on_op_proposal_create(transactions, hist, opdata, result, block_num, block_timestamp)
      when BitShares::Blockchain::Operations::Proposal_update.value
        on_op_proposal_update(transactions, hist, opdata, result, block_num, block_timestamp)
      when BitShares::Blockchain::Operations::Custom.value
        on_op_custom(transactions, hist, opdata, result, block_num, block_timestamp)
      when BitShares::Blockchain::Operations::Credit_offer_accept.value # => P2P抵押贷 借贷、还款、逾期
        on_op_credit_offer_accept(transactions, hist, opdata, result, block_num, block_timestamp)
      when BitShares::Blockchain::Operations::Credit_deal_repay.value
        on_op_credit_deal_repay(transactions, hist, opdata, result, block_num, block_timestamp)
      end
    end

    private def fmt_link_account(account_name) : String
      if url = url_config.try(&.url_explorer_account)
        return "<a href=\"#{sprintf(url, account_name)}\">#{account_name}</a>"
      else
        return account_name
      end
    end

    private def fmt_link_asset(asset_symbol) : String
      if url = url_config.try(&.url_explorer_asset)
        return "<a href=\"#{sprintf(url, asset_symbol)}\">#{asset_symbol}</a>"
      else
        return asset_symbol
      end
    end

    private def fmt_link_block(block_num) : String
      if url = url_config.try(&.url_explorer_block)
        return sprintf(url, block_num)
      else
        return "block_num: #{block_num}"
      end
    end

    private def fmt_link_txid(txid) : String
      if url = url_config.try(&.url_explorer_trx)
        return sprintf(url, txid)
      else
        return "txid: #{txid}"
      end
    end

    private def fmt_link_oid(oid) : String
      if url = url_config.try(&.url_explorer_oid)
        return "<a href=\"#{sprintf(url, oid)}\">##{oid}</a>"
      else
        return "##{oid}"
      end
    end

    private def fmt_asset_amount_item(json)
      fmt_asset_amount_item(json["amount"].to_i64, json["asset_id"].as_s)
    end

    private def fmt_asset_amount_item(amount : Int64, asset_id : String)
      asset = query_one_object(asset_id)
      return "#{BigDecimal.new(amount, asset["precision"].as_i)} #{fmt_link_asset(asset["symbol"].as_s)}"
    end

    private def account_id_to_name(account_id)
      return query_one_object(account_id)["name"].as_s
    end

    private struct BulkPush
      @app : App
      @feature_key : String
      @mark_account = {} of String => Bool

      def initialize(@app, @feature_key)
      end

      def <<(target_account_id : String)
        @mark_account[target_account_id] = true
      end

      def submit
        return if @mark_account.empty?

        chat_id_lang_hash = {} of String => String

        @mark_account.each do |target_account_id, _|
          @app.try_push?(target_account_id, @feature_key) { |chat_id, lang| chat_id_lang_hash[chat_id] = lang }
        end

        lang_hash = {} of String => Tuple(String, String)

        chat_id_lang_hash.each do |chat_id, lang|
          lang_hash[lang] = yield lang if !lang_hash.has_key?(lang)

          @app.push_to_user(*lang_hash[lang], chat_id)
        end
      end
    end

    private def on_op_transfer(transactions, hist, opdata, result, block_num, block_timestamp)
      to_id = opdata["to"].as_s

      try_push?(to_id, FeatureKeys::Transfer) do |chat_id, lang|
        from_id = opdata["from"].as_s

        # => 生成推送消息
        link_amount = fmt_asset_amount_item(opdata["amount"])
        link_from = fmt_link_account(account_id_to_name(from_id))
        link_to = fmt_link_account(account_id_to_name(to_id))
        link_txid = fmt_link_txid(calc_transaction_id(transactions, hist["trx_in_block"].as_i.to_u16))

        # => 格式：{from} 转账 {amount} 给 {to}。\n\n{txid}
        message = Lang.format(lang, :transfer_value,
          {
            from:   link_from,
            amount: link_amount,
            to:     link_to,
            txid:   link_txid,
          }
        )

        # => 推送
        push_to_user(Lang.text(lang, :transfer_title), message, chat_id)
      end
    end

    private def on_op_fill_order(transactions, hist, opdata, result, block_num, block_timestamp)
      # => 仅推送 maker 成交记录，taker 主动吃单行为不推送。
      return if !opdata["is_maker"].is_true?

      # => 同一个区块内 已经推送了则直接返回，不用重复推送。
      order_id = opdata["order_id"].as_s
      return if @push_mark_fill_order.has_key?(order_id)

      # => 标记
      @push_mark_fill_order[order_id] = true

      account_id = opdata["account_id"].as_s

      try_push?(account_id, FeatureKeys::Fill_order) do |chat_id, lang|
        # => 生成推送消息
        link_account = fmt_link_account(account_id_to_name(account_id))
        link_order_id = fmt_link_oid(order_id)
        link_txid = fmt_link_txid(calc_transaction_id(transactions, hist["trx_in_block"].as_i.to_u16))

        # => 格式：{account} 的订单 {order_id} 开始成交，请注意查看。\n\n{txid}
        message = Lang.format(lang, :fill_order_value,
          {
            account:  link_account,
            order_id: link_order_id,
            txid:     link_txid,
          }
        )

        # => 推送
        push_to_user(Lang.text(lang, :fill_order_title), message, chat_id)
      end
    end

    private def on_op_proposal_create(transactions, hist, opdata, result, block_num, block_timestamp)
      new_proposal_id = result.dig?(1).try(&.as_s?)
      if new_proposal_id.nil?
        Log.warn { "invalid operation_result: #{result}" }
        return
      end

      # => 查询并订阅提案
      new_proposal = subscriber.query_and_subscribe?(new_proposal_id)

      # => REMARK: 提案创建后立即批准完成了
      return if new_proposal.nil?

      # => 添加到监控列表
      @trace_deleted_ids.add(new_proposal_id)

      proposer_id = new_proposal["proposer"].as_s

      { {"required_owner_approvals", "owner"}, {"required_active_approvals", "active"} }.each do |tuple|
        new_proposal[tuple.first].as_a.each do |required_account_id|
          required_account = subscriber.query_and_subscribe!(required_account_id.as_s)
          proposer_account = subscriber.query_and_subscribe!(proposer_id)

          bulk_push = BulkPush.new(self, FeatureKeys::Proposal_create)

          # => 推送给 required_account 账号
          bulk_push << required_account_id.as_s

          # => 推送给所有参与多签的账号
          required_account.dig(tuple.last, "account_auths").as_a.each { |item| bulk_push << item[0].as_s }

          # => 提交
          bulk_push.submit do |lang|
            link_required = fmt_link_account(required_account["name"].as_s)
            link_proposer = fmt_link_account(proposer_account["name"].as_s)
            link_proposal_id = fmt_link_oid(new_proposal_id)
            link_txid = fmt_link_txid(calc_transaction_id(transactions, hist["trx_in_block"].as_i.to_u16))

            # => 格式：{required} 有新的提案 {proposal_id}，请注意查看。创建者：{proposer}。\n\n{txid}
            message = Lang.format(lang, :proposal_create_value,
              {
                required:    link_required,
                proposal_id: link_proposal_id,
                proposer:    link_proposer,
                txid:        link_txid,
              }
            )

            # => 返回推送文案
            {Lang.text(lang, :proposal_create_title), message}
          end
        end
      end
    end

    private def on_op_proposal_update(transactions, hist, opdata, result, block_num, block_timestamp)
      proposal_id = opdata["proposal"].as_s

      proposal = subscriber.query_and_subscribe?(proposal_id)

      # => REMARK: 用户批准后提案完成删除了 并且 本地缓存不存在（由于重启等原因 proposal_create 时缓存的数据丢失了。）
      if proposal.nil?
        Log.debug { "proposal missing ##{proposal_id}。" }
        return
      end

      # => 添加到监控列表
      @trace_deleted_ids.add(proposal_id)

      { {"required_owner_approvals", "owner"}, {"required_active_approvals", "active"} }.each do |tuple|
        proposal[tuple.first].as_a.each do |required_account_id|
          required_account = subscriber.query_and_subscribe!(required_account_id.as_s)

          bulk_push = BulkPush.new(self, FeatureKeys::Proposal_update)

          # => 批准提案不用推送给 required_account 账号
          # bulk_push << required_account_id.as_s

          # => 推送给所有参与多签的账号
          required_account.dig(tuple.last, "account_auths").as_a.each { |item| bulk_push << item[0].as_s }

          # => 提交
          bulk_push.submit do |lang|
            link_required = fmt_link_account(required_account["name"].as_s)
            link_proposal_id = fmt_link_oid(proposal_id)
            link_fee_paying_account = fmt_link_account(account_id_to_name(opdata["fee_paying_account"].as_s))
            link_txid = fmt_link_txid(calc_transaction_id(transactions, hist["trx_in_block"].as_i.to_u16))

            # => 两种文案
            # => 1、单个多签用户批准了提案。
            #       alice 批准了 xxx 的提案 #proposal_id，请注意查看。手续费账号：xxx。
            # => 2、撤销批准、或者 key 批准等其他情况。
            #       xxx 的提案 #proposal_id 有更新，请注意查看。手续费账号：xxx。
            owner_approvals_to_add = opdata["owner_approvals_to_add"].as_a
            active_approvals_to_add = opdata["active_approvals_to_add"].as_a

            message = if owner_approvals_to_add.size + active_approvals_to_add.size == 1
                        approval_account_id = if !owner_approvals_to_add.empty?
                                                owner_approvals_to_add[0].as_s
                                              else
                                                active_approvals_to_add[0].as_s
                                              end
                        link_approval_user = fmt_link_account(account_id_to_name(approval_account_id))

                        # => 格式：{approval_user} 批准了 {required} 的提案 {proposal_id}，请注意查看。手续费账号：{fee_paying_account}。\n\n{txid}
                        Lang.format(lang, :proposal_update_value01,
                          {
                            approval_user:      link_approval_user,
                            required:           link_required,
                            proposal_id:        link_proposal_id,
                            fee_paying_account: link_fee_paying_account,
                            txid:               link_txid,
                          }
                        )
                      else
                        # => 格式：{required} 的提案 {proposal_id} 有更新，请注意查看。手续费账号：{fee_paying_account}。\n\n{txid}
                        Lang.format(lang, :proposal_update_value02,
                          {
                            required:           link_required,
                            proposal_id:        link_proposal_id,
                            fee_paying_account: link_fee_paying_account,
                            txid:               link_txid,
                          }
                        )
                      end

            # => 返回推送文案
            {Lang.text(lang, :proposal_update_title), message}
          end
        end
      end
    end

    private def handle_credit_deal_repayment_date_reminder(deal_id, deal_object, seconds)
      try_push?(deal_object.borrower, FeatureKeys::Credit_deal_repay_time) do |chat_id, lang|
        # => 生成推送消息
        link_borrower = fmt_link_account(account_id_to_name(deal_object.borrower))
        link_debt = fmt_asset_amount_item(deal_object.debt_amount, deal_object.debt_asset)
        link_deal_id = fmt_link_oid(deal_id)

        # => 格式：{borrower} 的P2P借款 {debt} 还有 {hours} 小时逾期，请注意查看。{deal_id}
        message = Lang.format(lang, :credit_deal_repay_time_value,
          {
            borrower: link_borrower,
            debt:     link_debt,
            hours:    seconds // 3600,
            deal_id:  link_deal_id,
          }
        )

        # => 推送
        push_to_user(Lang.text(lang, :credit_deal_repay_time_title), message, chat_id)
      end
    end

    # => TODO: lang 这3个op本身是否有提醒？
    private def on_op_credit_offer_accept(transactions, hist, opdata, result, block_num, block_timestamp)
      # => extendable_operation_result
      # => [5, {"impacted_accounts" => ["1.2.25721"], "new_objects" => ["1.22.46"]}]
      new_deal_id = result.dig?(1, "new_objects", 0).try(&.as_s?)
      if new_deal_id.nil?
        Log.warn { "invalid operation_result: #{result}" }
        return
      end

      if new_deal_object = client.query_one_object(new_deal_id) # => query skip cache
        # => 添加到本地监控列表
        update_credit_deal_object_cache(new_deal_object, block_timestamp)
        Log.debug { "found a new deal object. ##{new_deal_id}" }
      else
        # => 借款后立即还款了，对象不存在。
        Log.debug { "found a new deal object, but repaid immediately, the deal object does not exist. ##{new_deal_id}" }
      end
    end

    # => TODO: lang 这3个op本身是否有提醒？
    private def on_op_credit_deal_repay(transactions, hist, opdata, result, block_num, block_timestamp)
      deal_id = opdata["deal_id"].as_s

      # => 更新 or 删除
      if deal_object = client.query_one_object(deal_id) # => query skip cache
        update_credit_deal_object_cache(deal_object, block_timestamp)
      else
        @monitor_all_credit_deal_object.delete(deal_id)
      end
    end

    # => 处理自定义数据，主要目的是在这里面处理用户配置动态更新，避免去订阅数据。
    private def on_op_custom(transactions, hist, opdata, result, block_num, block_timestamp)
      bin = opdata["data"].as_s.hexbytes
      return if bin.size == 0

      begin
        message = Graphene::Serialize::Pack(Graphene::Operations::T_custom_plugin_operation).unpack(bin)
      rescue e : Exception
        Log.warn { "custom operations plugin serializing error: #{e.message}, block_num: #{block_num}." }
        return
      end

      payer = opdata["payer"].as_s

      value = message.data.value
      case value
      in Graphene::Operations::T_account_storage_map
        case value.catalog
        when UserStorageCatalog_SocialMedia
          on_user_storage_social_media_changed(payer, value)
        when UserStorageCatalog_ReminderSettings
          on_user_storage_reminder_settings_changed(payer, value)
        else
          if payer == @config.verifier_account_id && value.catalog.index(AppStorageCatalog_SocialMediaVerifiedPrefix) == 0
            on_system_storage_social_media_verified_changed(payer, value)
          end
        end
      end
    end

    private def on_user_storage_social_media_changed(payer_account_id, item : Graphene::Operations::T_account_storage_map)
      # {
      #   id:      "7.0.xx",
      #   account: "1.2.#{target_account_id}",
      #   catalog: "bcdss.socialmedia.v1",
      #   key:     "telegram",
      #   value:   "usename",
      # }
      if item.remove
        item.key_values.each do |key_socialmedia_type, v|
          next if key_socialmedia_type != UserStorageKey_SocialMedia_telegram

          # => 用户删除了绑定的电报信息
          @all_connection_hash.delete(payer_account_id)

          Log.debug { ">>> #{account_id_to_name(payer_account_id)} delete social media." }
        end
      else
        item.key_values.each do |key_socialmedia_type, v|
          next if key_socialmedia_type != UserStorageKey_SocialMedia_telegram

          if json_string = v.value
            json = JSON.parse(json_string) rescue nil
            next if json.nil?

            connection_username = json.as_s?
            next if connection_username.nil?

            # => 更新绑定信息，如果更新失败（数据无效等）则删除绑定信息。
            if !update_user_social_media(payer_account_id, key_socialmedia_type, connection_username)
              @all_connection_hash.delete(payer_account_id)
            end

            Log.debug { ">>> #{account_id_to_name(payer_account_id)} update social media." }
          end
        end
      end
    end

    private def on_user_storage_reminder_settings_changed(payer_account_id, item : Graphene::Operations::T_account_storage_map)
      # {
      #   id:      "7.0.xx",
      #   account: "1.2.#{target_account_id}",
      #   catalog: "bcdss.reminder.settings.v1",
      #   key:     "app",
      #   value:   {
      #     "@provider": "xxx bot",
      #     "@lang":     "en",
      #     feature1:    true,
      #     feature2:    false,
      #   },
      # }
      if item.remove
        item.key_values.each do |key_app, v|
          next if key_app != UserStorageKey_ReminderSettings

          # => 用户删除了提醒配置项
          @all_reminder_settings.delete(payer_account_id)

          Log.debug { ">>> #{account_id_to_name(payer_account_id)} delete reminder setting." }
        end
      else
        item.key_values.each do |key_app, v|
          next if key_app != UserStorageKey_ReminderSettings

          if json_string = v.value
            json = JSON.parse(json_string) rescue nil

            # => 添加或修改提醒配置项，如果添加失败则删除。
            if !update_user_reminder_settings(payer_account_id, json)
              @all_reminder_settings.delete(payer_account_id)
            end

            Log.debug { ">>> #{account_id_to_name(payer_account_id)} update reminder setting." }
          end
        end
      end
    end

    private def on_system_storage_social_media_verified_changed(payer_account_id, item : Graphene::Operations::T_account_storage_map)
      # {
      #   id:      "7.0.xx",
      #   account: "1.2.#{verifier_account_id}",
      #   catalog: "bcdss.socialmedia.verified.v1.#{target_account_id}",
      #   key:     "dynamic_verify_key",
      #   value:   {
      #     account:     "target_account_id",
      #     socialmedia: "telegram",
      #     username:    "username",
      #     cid:         "chat_id",
      #   },
      # }
      if item.remove
        item.key_values.each do |key_verify_key, v|
          # => 删除认证信息
          if deleted_verified_data = @all_verified_hash.delete(key_verify_key)
            # => 删除之后同时更新用户当前绑定账号的认证信息
            update_user_social_media(key_verify_key, deleted_verified_data, false)

            Log.debug { ">>> system delete verify_key, #{account_id_to_name(deleted_verified_data.account_id)}." }
          end
        end
      else
        item.key_values.each do |key_verify_key, v|
          if json_string = v.value
            json = JSON.parse(json_string) rescue nil
            next if json.nil?

            # => 更新认证信息
            @all_verified_hash[key_verify_key] = verified_data = SSocialMediaVerified.new(json)

            # => 更新用户当前绑定账号的认证信息
            update_user_social_media(key_verify_key, verified_data, true)

            Log.debug { ">>> system update verify_key, #{account_id_to_name(verified_data.account_id)} => #{verified_data.socialmedia}:#{verified_data.username}." }
          end
        end
      end
    end

    private def update_user_reminder_settings(account_id : String, json_value : JSON::Any?) : Bool
      return false if json_value.nil?

      reminder_settings = json_value.as_h?
      return false if reminder_settings.nil?

      provider = reminder_settings[SpecSettingKey_Provider]?.try &.as_s?
      lang = reminder_settings[SpecSettingKey_Language]?.try &.as_s?
      return false if provider.nil? || lang.nil? || provider.empty? || lang.empty?

      item = SReminderSettings.new(provider, lang)

      # => 初始化默认值
      @reminder_feature_default_value.each { |k, v| item.set_feature_flag(k, v) }

      # => 合并用户设置
      reminder_settings.each { |k, v| item.set_feature_flag(k, v.is_true?) if k != SpecSettingKey_Provider && k != SpecSettingKey_Language }

      @all_reminder_settings[account_id] = item

      return true
    end

    # => 用户绑定的电报账号变更：更新当前连接信息
    private def update_user_social_media(account_id : String, socialmedia : String, connection_username : String) : Bool
      return false if connection_username.empty?

      # => 添加 or 更新
      verify_key = gen_social_media_verified_key(account_id, socialmedia, connection_username)

      @all_connection_hash[account_id] = if verified_data = @all_verified_hash[verify_key]?
                                           SSocialMediaConnection.new(verify_key, connection_username, true, verified_data.chat_id)
                                         else
                                           SSocialMediaConnection.new(verify_key, connection_username, false, nil)
                                         end
      return true
    end

    # => 认证信息发生变化：尝试更新用户当前连接信息
    private def update_user_social_media(verify_key : String, verified_data : SSocialMediaVerified, is_verified : Bool) : Bool
      account_id = verified_data.account_id

      connection_data = @all_connection_hash[account_id]?
      return false if connection_data.nil?
      return false if connection_data.verify_key != verify_key

      @all_connection_hash[account_id] = if is_verified
                                           SSocialMediaConnection.new(verify_key, connection_data.username, true, verified_data.chat_id)
                                         else
                                           SSocialMediaConnection.new(verify_key, connection_data.username, false, nil)
                                         end
      return true
    end

    private def update_credit_deal_object_cache(deal_object, init_block_time : UInt32)
      # {"id"                => "1.22.410",
      #  "borrower"          => "1.2.1014025",
      #  "offer_id"          => "1.21.220",
      #  "offer_owner"       => "1.2.1795137",
      #  "debt_asset"        => "1.3.3291",
      #  "debt_amount"       => 5200000,
      #  "collateral_asset"  => "1.3.0",
      #  "collateral_amount" => 52000,
      #  "fee_rate"          => 2500,
      #  "latest_repay_time" => "2022-08-01T15:34:48"}
      deal_id = deal_object["id"].as_s
      borrower = deal_object["borrower"].as_s
      debt_asset = deal_object["debt_asset"].as_s
      debt_amount = deal_object["debt_amount"].to_i64
      latest_repay_time = BitShares::Utility.parse_time_string_i64(deal_object["latest_repay_time"].as_s)

      @monitor_all_credit_deal_object[deal_id] = SRuntimeCreditDealObject.new(borrower, debt_asset, debt_amount, latest_repay_time.to_u32, init_block_time)
    end

    # => 初始化所有社交媒体绑定信息、认证信息以及提醒配置项
    private def init_data_once
      return if !@all_reminder_settings.empty?
      return if !@all_verified_hash.empty?
      return if !@all_connection_hash.empty?

      # => 1、初始化所有提醒配置项
      Log.info { "load all reminder settings." }
      query_storage_by_catalog(UserStorageCatalog_ReminderSettings).each do |storage_item|
        next if storage_item["key"]?.try(&.as_s?) != UserStorageKey_ReminderSettings

        update_user_reminder_settings(storage_item["account"].as_s, storage_item["value"]?)
      end

      # => 2、初始化所有认证信息
      Log.info { "load all socialmedia verified data." }
      query_storage_by_account(@config.verifier_account_id).each do |storage_item|
        if storage_item["catalog"].as_s.index(AppStorageCatalog_SocialMediaVerifiedPrefix) == 0
          @all_verified_hash[storage_item["key"].as_s] = SSocialMediaVerified.new(storage_item["value"])
        end
      end

      # => 3、初始化所有用户绑定信息
      Log.info { "load all user socialmedia data." }
      query_storage_by_catalog(UserStorageCatalog_SocialMedia).each do |storage_item|
        socialmedia = storage_item["key"]?.try(&.as_s?)
        next if socialmedia.nil? || socialmedia != UserStorageKey_SocialMedia_telegram

        connection_username = storage_item["value"]?.try &.as_s?
        next if connection_username.nil?

        update_user_social_media(storage_item["account"].as_s, socialmedia, connection_username)
      end
    end

    # => 初始化所有 P2P抵押贷的 deal objecct 信息
    private def init_all_credit_deal_object_once(init_block_time : UInt32)
      return if !@monitor_all_credit_deal_object.empty?

      Log.info { "load all credit deal object." }
      api_limit = 50 # => TODO:api limit ???
      start_id = "1.22.0"

      loop do
        # => 查询数据
        data_array = client.call_db("list_credit_deals", [api_limit, start_id]).as_a

        # => 处理数据
        data_array.each { |deal_object| update_credit_deal_object_cache(deal_object, init_block_time) }

        # => 是否查询完成判断
        if data_array.size < api_limit
          # => no more data
          break
        else
          # => 继续查询
          last_id = data_array.last["id"].as_s.split(".").last.to_i
          start_id = "1.22.#{last_id + 1}"
        end
      end
    end

    # 生成社交媒体绑定时候需要验证的 KEY 字段信息。
    #
    # MARK: 这个需要和验证服务器保持一致，不能随意修改。
    private def gen_social_media_verified_key(account_id : String, socialmedia : String, value : String) : String
      digest_string = "#{account_id}#{socialmedia}#{value}"

      return BitShares::Utility.sha512_hex(digest_string)
    end

    private def query_storage_by_account(account_id : String)
      # => TODO: 缺少 api 测试临时获取前 500 个对象
      client.call_db("get_objects", [(0..500).map { |i| "7.0.#{i}" }]).as_a.select { |storage_item| storage_item && storage_item.raw && storage_item["account"].as_s == account_id }
    end

    private def query_storage_by_catalog(catalog : String)
      # => TODO: 缺少 api 测试临时获取前 500 个对象
      client.call_db("get_objects", [(0..500).map { |i| "7.0.#{i}" }]).as_a.select { |storage_item| storage_item && storage_item.raw && storage_item["catalog"].as_s == catalog }
    end

    private def query_storage(account_id, catalog)
      client.call_custom_operations("get_storage_info", [account_id, catalog]).as_a
    end

    private def query_one_object(oid : String, skip_cache = false)
      result = if skip_cache
                 client.query_one_object(oid)
               else
                 client.cache.query_one_object(oid)
               end
      return result.not_nil!
    end

    # 异常处理
    private def on_exception(e)
      Log.error(exception: e) { "unknown error" }

      case e
      when BitShares::TimeoutError
        # => 不处理，下次直接重连。
      when BitShares::SocketClosed
        # => 不处理，下次直接重连。
      when BitShares::ResponseError
        api_error = e.graphene_error_message

        Log.error { api_error }

        # => 反馈异常
        push_to_administrator(e, {api_error: api_error})
      else
        # => 反馈异常
        push_to_administrator(e)
      end
    end
  end
end
