module AppPusher
  module Lang
    extend self

    # 简体中文
    Lang_zh_s = {
      transfer_title: "【新的收款】",
      transfer_value: "{from} 转账 {amount} 给 {to}。\n\n{txid}",

      asset_issue_title: "【新的收款】",
      asset_issue_value: "{from} 发行 {amount} 给 {to}。\n\n{txid}",

      fill_order_title: "【订单成交】",
      fill_order_value: "{account} 的订单 {order_id} 开始成交，请注意查看。\n\n{txid}",

      proposal_create_title: "【新的提案】",
      proposal_create_value: "{required} 有新的提案 {proposal_id}，请注意查看。创建者：{proposer}。\n\n{txid}",

      proposal_update_title:   "【更新提案】",
      proposal_update_value01: "{approval_user} 批准了 {required} 的提案 {proposal_id}，请注意查看。手续费账号：{fee_paying_account}。\n\n{txid}",
      proposal_update_value02: "{required} 的提案 {proposal_id} 有更新，请注意查看。手续费账号：{fee_paying_account}。\n\n{txid}",

      credit_offer_accept_title: "【P2P抵押借款】",
      credit_offer_accept_value: "{borrower} 从P2P抵押贷 {offer_id} 借款 {amount}，抵押物 {collateral}。\n\n{txid}",

      credit_deal_repay_title: "【P2P抵押还款】",
      credit_deal_repay_value: "{borrower} 归还 {amount} P2P抵押贷 {offer_id} 的借款，手续费 {fee}。\n\n{txid}",

      credit_deal_repay_time_title: "【P2P借款即将逾期】",
      credit_deal_repay_time_value: "{borrower} 的P2P借款 {debt} 还有 {hours} 小时逾期，请注意查看。{deal_id}",

      samet_fund_borrow_title: "【闪电贷借款】",
      samet_fund_borrow_value: "{borrower} 从闪电贷 {fund_id} 借款 {amount}。\n\n{txid}",
    }

    # 英文
    Lang_en = {
      transfer_title: "[TRANSFER]",
      transfer_value: "{from} sent {amount} to {to}.\n\n{txid}",

      asset_issue_title: "[ASSET ISSUE]",
      asset_issue_value: "{from} issued {amount} to {to}。\n\n{txid}",

      fill_order_title: "[FILL ORDER]",
      fill_order_value: "the order {order_id} of {account} has been filled, please check it out. \n\n{txid}",

      proposal_create_title: "[NEW PROPOSAL]",
      proposal_create_value: "{required} has a new proposal {proposal_id}, please check it out. proposer: {proposer}. \n\n{txid}",

      proposal_update_title:   "[UPDATE PROPOSAL]",
      proposal_update_value01: "{approval_user} approved {required}'s proposal {proposal_id}, please check it out. fee account: {fee_paying_account}.\n\n{txid}",
      proposal_update_value02: "{required}'s proposal {proposal_id} has been updated, please check it out. fee account: {fee_paying_account}. \n\n{txid}",

      credit_offer_accept_title: "[BORROW FROM CREDIT OFFER]",
      credit_offer_accept_value: "{borrower} borrows {amount} from credit offer {offer_id}, collateral {collateral}. \n\n{txid}",

      credit_deal_repay_title: "[REPAY TO CREDIT OFFER]",
      credit_deal_repay_value: "{borrower} repay {amount} to credit offer {offer_id}, fee {fee}. \n\n{txid}",

      credit_deal_repay_time_title: "[CREDIT DEAL IS ABOUT TO EXPIRE]",
      credit_deal_repay_time_value: "{borrower}'s credit deal {debt} is overdue for {hours} hours, please check it out. {deal_id}",

      samet_fund_borrow_title: "[BORROW FROM FLASH LOAN]",
      samet_fund_borrow_value: "{borrower} borrows {amount} from flash loan {fund_id}. \n\n{txid}",
    }

    # 日文 TODO: 目前为空
    Lang_jp = NamedTuple.new

    private Lang_all = {
      "zh-s": Lang_zh_s,
      "en":   Lang_en,
      "jp":   Lang_jp,
    }

    # => 动态多语言默认优先级，对应 lang-key 字符串不存在的情况下，则按照以下顺序查找，都不存在则返回 lang-key 字符串本身。
    private Dynamic_lang_priority = {"zh-s", "en"}

    def format(lang_key : String, str_key, args : NamedTuple)
      fmt = text(lang_key, str_key)
      if args.empty?
        return fmt
      else
        return fmt.gsub(/{(\w+?)}/) { |match_str| args[$1]? || match_str }
      end
    end

    def format(lang_key : String, str_key, **args)
      return format(lang_key, str_key, args)
    end

    def text(lang_key : String, str_key) : String
      # => 查询指定语言
      if value = get_text_core(lang_key, str_key)
        return value
      end

      # => 尝试从默认语言列表查询
      Dynamic_lang_priority.each do |default_lang_key|
        # => 已经查询过了，跳过。
        next if default_lang_key == lang_key

        if value = get_text_core(default_lang_key, str_key)
          return value
        end
      end

      # => 未找到：返回 str key 自身。
      return str_key.to_s
    end

    private def get_text_core(lang_key : String, str_key) : String?
      return Lang_all[lang_key]?.try &.[]?(str_key)
    end
  end
end
