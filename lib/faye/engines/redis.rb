module Faye
  module Engine

    class Redis < Base
      DEFAULT_HOST     = 'localhost'
      DEFAULT_PORT     = 6379
      DEFAULT_DATABASE = 0
      DEFAULT_GC       = 60
      LOCK_TIMEOUT     = 120
      def initialize(options)
        require 'em-hiredis'
        super
        host = options[:host]       || DEFAULT_HOST
        port = options[:port]       || DEFAULT_PORT
        db   = options[:database]   || 0
        password = options[:password]
        @gc_interval = options[:gc] || DEFAULT_GC
        @ns  = options[:namespace]  || 'faye'

        @redis = ::Redis.new(:host => host, 
                           :port => port, 
                           :password => password, 
                           :db => db)

        @gc_runner = EM.add_periodic_timer(@gc_interval, &method(:gc))

        @subscriber = EM::Hiredis::Client.connect(host, port)
        @subscriber.auth(password) if password
        @subscriber.select(db)
        start_subscribe
      end

      def start_subscribe 
        @subscriber.psubscribe "#{@ns}/channels/*"
        @subscriber.on(:pmessage) do |_, topic, json_msg|
          debug 'Message to ?', topic
          msg = JSON.parse(json_msg)
          channel = topic.sub "#{@ns}/channels/", ''
          debug 'Sending msg to all subscribers of ?', channel
          clients = @redis.smembers channel
          clients.each do |client_id|
            next unless conn = connection(client_id, false)
            conn.deliver msg
          end
        end
      end

      def empty_queue(queue)
        debug 'Empty queue ?, noop', queue
      end

      def publish(message)
        debug 'Publishing message ?', message
        json_message = JSON.dump(message)
        channels = Channel.expand(message['channel'])
        channels.each do |channel|
          @redis.publish "#{@ns}/channels/#{channel}", json_message
        end
      end

      def disconnect
        debug 'Disconnecting'
        EM.cancel_timer @gc_runner
        @redis.quit
        @subscriber.quit
      end

      def create_client(&callback)
        client_id = Faye.random
        ping client_id
        debug 'Created new client ?', client_id
        callback.call client_id
      end

      def destroy_client(&callback)
        debug 'Destroyed client ?', client_id
      end

      def client_exists(client_id, &callback)
        debug 'Does client ? exists', client_id
        @redis.exists client_id
        score = @redis.zscore "#{@ns}/clients", client_id
        callback.call score != nil
      end

      def ping(client_id)
        debug 'Ping ?, ?', client_id, Time.now.to_i
        @redis.zadd "#{@ns}/clients", Time.now.to_i, client_id
      end

      def subscribe(client_id, channel, &callback)
        debug 'Subscribed client ? to channel ?', client_id, channel
        @redis.sadd "#{@ns}/clients/#{client_id}", channel
        @redis.sadd "#{@ns}/channels/#{channel}", client_id
        callback.call
      end

      def unsubscribe(client_id, channel, &callback)
        debug 'Unsubscribed client ? from channel ?', client_id, channel
        @redis.srem "#{@ns}/clients/#{client_id}", channel
        @redis.srem "#{@ns}/channels/#{channel}", client_id
        callback.call
      end

      private
      def gc
        last_gc = @redis.get "#{@ns}/last_gc" 
        return if last_gc.to_i > Time.now.to_i - @gc_interval

        @redis.watch "#{@ns}/clients"
        @redis.watch "#{@ns}/last_gc"

        cutoff = Time.now.to_i - 2 * @timeout
        clients = @redis.zrangebyscore("#{@ns}/clients", 0, cutoff)
        unless clients.any?
          @redis.set "#{@ns}/last_gc", Time.now.to_i
          return
        end

        channels = @redis.keys "#{@ns}/channels/*"
        @redis.multi 
        @redis.zremrangebyscore("#{@ns}/clients", 0, cutoff)
        channels.each do |channel|
          # do this when redis-rb is updated to >2.2.2 
          # @redis.srem channel, *clients
          clients.each do |client|
            @redis.srem channel, client
          end
        end
        @redis.set "#{@ns}/last_gc", Time.now.to_i
        @redis.exec
      end

    end

    register 'redis', Redis
  end
end
