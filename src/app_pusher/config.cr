module AppPusher
  class Config
    property api_node : String | Array(String)  # => [必填] 水龙头链接的API节点地址，API地址 或 API地址数组。
    property telegram_bot_token : String        # => [必填] 电报机器人 token
    property telegram_private_chat_id : String? # => [可选] 接受异常推送的个人电报 chat id。
    property verifier_account_id : String       # => [必填] 负责验证的账号ID，用于登记核验信息的账号。
    property app_settings_account_id : String?  # => [可选] BTS++ APP链上存储配置信息的账号ID，用于读取提醒设置的默认值。为空则不读取默认值。
    property log_to_file = false                # => [可选] 是否记录日志到文件，默认 false，可用 --debug 参数开启。

    def initialize(@api_node, @telegram_bot_token, @telegram_private_chat_id, @verifier_account_id, @app_settings_account_id)
    end
  end
end
