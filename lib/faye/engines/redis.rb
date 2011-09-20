module Faye
  module Engine

    class Redis < Base
      DEFAULT_HOST     = 'localhost'
      DEFAULT_PORT     = 6379
      DEFAULT_DATABASE = 0
      DEFAULT_GC       = 60
      LOCK_TIMEOUT     = 120
      def initialize(options)
        super
        host = @options[:host]       || DEFAULT_HOST
        port = @options[:port]       || DEFAULT_PORT
        db   = @options[:database]   || 0
        password = @options[:password]
        @gc_interval = @options[:gc] || DEFAULT_GC
        @ns  = @options[:namespace]  || 'faye'

        @redis = Redis.new(:host => host, 
                           :port => port, 
                           :password => password, 
                           :db => db)
        @subscriber = Redis.new(:host => host, 
                                :port => port, 
                                :password => password, 
                                :db => db)

        @gc_runner = EventMachine.add_periodic_timer(@gc_interval, &method(:gc))
      end

      def subscribe 
        @subscriber.psubscribe "#{@ns}/channels/*" do |on|
          on.message do |channel, json_msg|
            msg = JSON.parse(json_msg)
            faye_channel = channel.split('/').last
            clients = @redis.smembers faye_channel 
            clients.each do |client_id|
              next unless conn = connection(client_id, false)
              conn.deliver msg
            end
          end
        end
      end

      def publish(message)
        json_message = JSON.dump(message)
        channels = Channel.expand(message['channel'])
        channels.each do |channel|
          @redis.publish "#{@ns}/channels/#{channel}", message
        end
      end

      def disconnect
        EM.cancel_timer @gc_runner
        @redis.quit
        @subscriber.quit
      end

      def create_client(&callback)
        client_id = Faye.random
        ping client_id
      end

      def client_exists(client_id, &callback)
        @redis.exists client_id
        score = @redis.zscore "#{@ns}/clients", client_id
        callback.call score != nil
      end

      def ping(client_id)
        @redis.zadd "#{@ns}/clients", Time.now.to_i, client_id
        callback.call client_id
      end

      def subscribe(client_id, channel, &callback)
        @redis.sadd "#{@ns}/clients/#{client_id}", channel
        @redis.sadd "#{@ns}/channels/#{channel}", client_id
        callback.call
      end

      def unsubscribe(client_id, channel, &callback)
        @redis.srem "#{@ns}/clients/#{client_id}", channel
        @redis.srem "#{@ns}/channels/#{channel}", client_id
        callback.call
      end

      def gc
        return if @redis.get "#{@ns}/last_gc" > Time.now.to_i - @gc_interval

        @redis.watch "#{@ns}/clients"
        @redis.watch "#{@ns}/last_gc"

        cutoff = Time.now.to_i - 2 * @timeout
        clients = @redis.zrangebyscore("#{@ns}/clients", 0, cutoff)
        channels = @redis.keys "#{@ns}/channels/*"
        @redis.multi 
        @redis.zremrangebyscore("#{@ns}/clients", 0, cutoff)
        channels.each do |channel|
          @redis.srem "#{@ns}/channels/#{channel}", *clients
        end
        @redis.set "#{@ns}/last_gc", Time.now.to_i
        @redis.exec
      end

    end
    register 'redis', Redis
  end
end
