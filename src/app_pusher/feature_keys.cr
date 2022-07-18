module AppPusher
  # 支持的提醒功能 REMARK: 需要和 common.settings.ver.x 配置里的 reminder_data 特性名字一致。
  module FeatureKeys
    Transfer               = "transfer"               # => 收款通知
    Fill_order             = "fill_order"             # => 订单成交 maker
    Credit_deal_repay_time = "credit_deal_repay_time" # => P2P抵押贷还款日期临近提醒
  end
end
