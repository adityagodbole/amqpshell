require 'amqp'
require 'base64'
require 'pathname'
require 'fileutils'

module AmqpShell
  class QueueIO
    attr_reader :queue
    def initialize(queue,mode,id,jobid)
      @mode = mode
      @queue = queue
      @eof = false
      @nodeid = id
      @jobid = id
      @subscriptions = []
      @subscribed = false
    end

    def on_data(&blk)
      raise "InvalidMode" if @mode != 'r'
      @subscriptions << blk
      return if @subscribed
      @queue.subscribe do |header,message|
        header = header.headers
        @subscriptions.each {|b| b.call(Base64.decode64(message))}
        if header['eof']
          @queue.unsubscribe
          @eof = true
        end
      end
      @subscribed = true
    end

    def eof?
      @eof
    end

    def send(msg, header = {})
      return if msg.empty?
      raise "EOF" if eof?
      raise "InvalidMode" if @mode != 'w'
      @queue.publish(Base64.encode64(msg), {:headers => {'nodeid' => @nodeid,
                     'jobid' => @jobid}.merge(header)})
      if header['eof']
        @queue.unbind
        @eof = true
      end
    end

    def close
      begin
        @queue.unsubscribe
      rescue
      end
    end

    def delete
      begin
        @queue.delete
      rescue
      end
    end
  end

end
