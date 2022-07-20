module AppPusher
  # 支持的提醒功能 REMARK: 需要和 common.settings.ver.x 配置里的 reminder_data 特性名字一致。
  module FeatureKeys
    Transfer               = "transfer"               # => 新的收款
    Fill_order             = "fill_order"             # => 订单成交 maker
    Credit_deal_repay_time = "credit_deal_repay_time" # => P2P借款即将逾期
    Proposal_create        = "proposal_create"        # => 新的提案
  end
end