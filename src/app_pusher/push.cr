require "http/client"
require "json"
require "log"

module AppPusher
  # 参考1: https://core.telegram.org/bots
  # 参考2: https://core.telegram.org/bots/api
  class TelegramPushService
    Log = ::Log.for("push")

    # 格式参考：https://core.telegram.org/bots/api#sendmessage
    enum TextFormat : UInt8
      TEXT
      MARK_DOWN
      HTML
    end

    MAX_RETRY_NUMBER = 3 # => 发送失败最大尝试次数

    struct MsgItem
      getter chat_id : String
      getter msg : String
      getter fmt : TextFormat

      property retry : UInt8 = 0_u8

      def initialize(@chat_id, @msg, @fmt)
      end
    end

    struct Statistics
      property success : UInt64
      property failed : UInt64

      def initialize(@success = 0_u64, @failed = 0_u64)
      end
    end

    getter statistics = Statistics.new  # => 简单的统计数据
    getter bot_username : String        # => 电报机器人 username
    @bot_token : String                 # => 电报机器人 token
    @pending_queue = Deque(MsgItem).new # => 待推送的消息队列

    def initialize(@bot_token)
      if bot_username = getMe.try(&.dig?("result", "username").try(&.as_s))
        @bot_username = bot_username
        Log.info { "init bot username success: #{@bot_username}" }
      else
        raise "init bot username failed: invalid bot token"
      end

      spawn { run }
    end

    # 推送：代码异常信息
    def add_task(target_chat_id : String, e : Exception, additional_data = nil)
      error_message = if additional_data
                        "[Exception]\n#{e.detail_message}\n\n#{additional_data.pretty_inspect}\n"
                      else
                        "[Exception]\n#{e.detail_message}"
                      end
      add_task(target_chat_id: target_chat_id, message: error_message)
    end

    # 推送：普通信息
    def add_task(target_chat_id : String, message : String, format : TextFormat = :text)
      @pending_queue << MsgItem.new(chat_id: target_chat_id, msg: message, fmt: format)
    end

    private def run
      loop do
        if item = @pending_queue.shift?
          if sendMessage(item.chat_id, item.msg, item.fmt)
            @statistics.success += 1
            Log.debug { "push to telegram success, chat_id: #{item.chat_id}" }
          else
            @statistics.failed += 1
            Log.debug { "push to telegram failed, chat_id: #{item.chat_id}" }

            # => 失败：考虑重新发送
            item.retry += 1
            @pending_queue << item if item.retry < MAX_RETRY_NUMBER
          end
        else
          sleep(0.005)
        end
      end
    end

    private def getMe
      return post("getMe")
    end

    private def sendMessage(target_chat_id, message, format : TextFormat)
      case format
      when .text?
        sendMessageText(target_chat_id, message)
      when .mark_down?
        sendMessageMD(target_chat_id, message)
      when .html?
        sendMessageHTML(target_chat_id, message)
      end
    end

    private def sendMessageText(target_chat_id, message)
      return post("sendMessage", {chat_id: target_chat_id, text: message})
    end

    private def sendMessageMD(target_chat_id, markdown_message)
      return post("sendMessage", {chat_id: target_chat_id, text: markdown_message, parse_mode: "Markdown"})
    end

    private def sendMessageHTML(target_chat_id, html)
      return post("sendMessage", {chat_id: target_chat_id, text: html, parse_mode: "HTML"})
    end

    private def post(method, args = nil)
      uri = URI.parse("https://api.telegram.org/bot#{@bot_token}/#{method}")

      client = HTTP::Client.new(uri)
      client.dns_timeout = 3.0
      client.connect_timeout = 3.0
      client.read_timeout = 5.0
      client.write_timeout = 5.0

      resp = client.post(uri.request_target, headers: HTTP::Headers{"Content-Type" => "application/json"}, body: args.try(&.to_json))

      if resp.status_code != 200
        Log.warn { "telegram api error: #{resp.body} code: #{resp.status_code}" }
        return nil
      end

      json = JSON.parse(resp.body)
      unless json["ok"]?.try(&.as_bool?)
        Log.warn { "telegram api error: #{json}" }
        return nil
      end

      return json
    rescue e : Exception
      Log.error(exception: e) { "telegram api error" }
      return nil
    end
  end
end
