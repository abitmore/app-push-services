module AppPusher
  class ObjectSubscriber
    private DYNAMIC_GLOBAL_PROPERTY_ID = "2.1.0" # => 链全局属性ID，包含最新区块编号，时间等信息。

    getter disconnected = false                           # => 连接是否断开
    @subscribe_objects_hash = Hash(String, JSON::Any).new # => 订阅对象
    @client : BitShares::Client                           # => client 对象

    def initialize(@client)
    end

    def start
      # => 重新连接时候清理数据
      @subscribe_objects_hash.clear
      @disconnected = false

      # => 设置订阅 callback
      set_subscribe_callback = ->(success : Bool, data : JSON::Any | String) {
        if success
          handle_chain_notify(data) if data.is_a?(JSON::Any)
          return false
        else
          # => 通知：连接断开
          @disconnected = true
          return true
        end
      }
      @client.call_db("set_subscribe_callback", [false], callback: set_subscribe_callback)

      # => 默认订阅对象
      query_and_subscribe?(DYNAMIC_GLOBAL_PROPERTY_ID)
    end

    # 从缓存删除
    def delete_object(oid)
      @subscribe_objects_hash.delete(oid)
    end

    # 直接从缓存获取对象
    def get_object?(oid : String)
      return @subscribe_objects_hash[oid]?
    end

    def get_object!(oid : String)
      get_object?(oid).not_nil!
    end

    # 查询并订阅对象
    def query_and_subscribe?(oid : String)
      if obj = @subscribe_objects_hash[oid]?
        return obj
      else
        @client.call_db("get_objects", [{oid}, true]).as_a.each { |obj| @subscribe_objects_hash[obj["id"].as_s] = obj }
        return @subscribe_objects_hash[oid]?
      end
    end

    def query_and_subscribe!(oid : String)
      query_and_subscribe?(oid).not_nil!
    end

    private def handle_chain_notify(data : JSON::Any?)
      return if data.nil?

      case raw_data = data.raw
      when Array
        raw_data.as(Array).each { |sub_data| handle_chain_notify(sub_data) }
      when Hash
        handle_chain_notify_core(data)
      end
    end

    private def handle_chain_notify_core(obj)
      return if obj.nil?

      oid = obj["id"]?.try(&.as_s?)
      return if oid.nil?

      @subscribe_objects_hash[oid] = obj

      Log.debug { "##{oid} has been updated." } if oid != DYNAMIC_GLOBAL_PROPERTY_ID
    end
  end
end
