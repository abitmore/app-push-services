require "./app_pusher/**"

require "bitshares-crystal"
require "crystal-secp256k1-zkp"
require "option_parser"

require "log"

# --------------------------------------------------------------------------
# 启动
# --------------------------------------------------------------------------
def start_app(api_node : String | Array(String),        # => API节点
              verifier_account_id : String,             # => 核验者账号ID
              telegram_bot_token : String = "",         # => 电报机器人 token
              telegram_private_chat_id : String? = nil, # => 运维人员接受异常信息的 chat_id
              app_settings_account_id : String? = nil   # => BTS++链上配置信息数据存储账号ID
              )
  # => 配置信息
  app_config = AppPusher::Config.new(api_node, telegram_bot_token, telegram_private_chat_id, verifier_account_id, app_settings_account_id)

  # => 命令行参数解析
  OptionParser.parse do |parser|
    parser.banner = "Usage:"

    # => 参数：API节点，多个节点用逗号分隔
    parser.on("-s API", "--server API", "Specify the websocket server api.") do |opt|
      app_config.api_node = opt.split(",")
    end

    parser.on("--verifier ACCOUNT_ID", "Specify verifier account id.") do |opt|
      app_config.verifier_account_id = opt
    end

    parser.on("--app-settings ACCOUNT_ID", "Specify btspp app settings account id.") do |opt|
      app_config.app_settings_account_id = opt
    end

    # => 参数：快捷连接BTS主网络，配置API节点、验证者账号ID和APP链上配置信息账号ID。
    parser.on("--mainnet", "Connect to the BitShares main network.") do |opt|
      app_config.api_node = "wss://api.bts.btspp.io:10100" # => mainnet api
      app_config.verifier_account_id = "1.2.1814430"       # => btspp-app-verifier
      app_config.app_settings_account_id = "1.2.1678327"   # => btspp-app-settings
    end

    # => 参数：快捷连接BTS测试网络，配置API节点、验证者账号ID和APP链上配置信息账号ID。
    parser.on("--testnet", "Connect to the BitShares test network.") do |opt|
      app_config.api_node = "ws://101.35.27.58:10099"  # => testnet api
      app_config.verifier_account_id = "1.2.25959"     # => app-settings
      app_config.app_settings_account_id = "1.2.25959" # => app-settings
    end

    # => 参数：电报机器人 token
    parser.on("--token BOT_TOKEN", "Specify telegram bot token.") do |opt|
      app_config.telegram_bot_token = opt
    end

    # => 参数：个人电报 chat_id，用于接受部分反馈信息。
    parser.on("--chat STRING", "Specify private chat id.") do |opt|
      app_config.telegram_private_chat_id = opt
    end

    # => 参数：更改日志级别为 DEBUG。所有日志输出到 STDIO，DEBUG以上日志记录文件。
    parser.on("--debug", "Set the log level to debug.") do |opt|
      app_config.log_to_file = true

      Log.setup do |c|
        c.bind "*", :trace, ::Log::IOBackend.new
        c.bind "*", :debug, FileBackend.new("app_pusher.debug.log")
        c.bind "*", :warn, FileBackend.new("app_pusher.warn.log")
        c.bind "*", :error, FileBackend.new("app_pusher.error.log")
      end
    end

    # => 参数：帮助信息
    parser.on("-h", "--help", "Show this help") do
      puts parser
      exit
    end

    # => 参数：版本信息
    parser.on("-v", "--version", "Show app version") do
      puts "Version: #{AppPusher::VERSION}"
      exit
    end

    parser.invalid_option do |flag|
      STDERR.puts "ERROR: #{flag} is not a valid option."
      STDERR.puts "\n"
      STDERR.puts parser
      exit(1)
    end
  end

  # => 检测参数有效性
  if app_config.telegram_bot_token.empty?
    puts "The telegram bot token args is invalid, you can specify it with the --token parameter."
    exit
  end

  # => 启动主程序
  AppPusher::App.new(app_config).run
end

# => 连接主网启动 REMARK: 可使用 --testnet 连接测试网
start_app(
  api_node: "wss://api.bts.btspp.io:10100", # => btspp.io api
  verifier_account_id: "1.2.1814430",       # => btspp-app-verifier
  app_settings_account_id: "1.2.1678327",   # => btspp-app-settings
)
