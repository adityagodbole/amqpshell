dir = File.dirname(__FILE__)
require File.expand_path(dir + '/queueio.rb')
require File.expand_path(dir + '/utils.rb')

# Client side classes
# RemoteJob - Class representing a job running on a remote node
# RemoteRunner - Class for housekeeping remote jobs and running the amqp event loop
module AmqpShell

  class RemoteJob
    attr_reader :jobid, :jobheader, :fs, :retheader, :retcode, :destkey, :state
    attr_reader :stdin, :stdout, :stderr
    @run_lock
    def initialize(manager,destination)
      @manager = manager
      @exchange = manager.exchange
      @channel = manager.channel
      @destkey = destination
      @jobid = make_rand_str(16)
      @id = manager.id
      @state = 'init'
      @run_lock = Mutex.new
      @stdin = @inqueue = QueueIO.new(@channel.queue(@jobid + "in"),'w',@id,@jobid)
      @stdout = @outqueue = QueueIO.new(@channel.queue(@jobid + "out"),'r',@id,@jobid)
      @stderr = @errqueue = QueueIO.new(@channel.queue(@jobid + "err"),'r',@id,@jobid)
      @retcode = nil
      # todo: add a destructor to delete queues
    end

    def setupIO(&blk)
      blk.call(@inqueue,@outqueue,@errqueue,self)
    end

    def run_job(jobheader, fs = "", timeout = 0, &blk)
      fs ||= ""
      jobheader['jobid'] = @jobid
      @jobheader = jobheader
      @fs = fs
      blk_given = block_given?
      EM.schedule do
        @run_lock.lock
        publish(@jobheader, fs, @destkey, timeout) do |header,fs|
          next if !@run_lock.locked?
          $Logger.debug "#{@jobid} -> #{header}"
          if header['state'] == 'finished'
            @run_lock.unlock
            @fs = Base64.decode64(fs)
            @retcode = header['return']
          end
          @state = header['state'] || 'unknown'
          blk.call(self) if blk_given
          if @state == 'finished'
            self.destroy
          end
        end
        @state = 'pending'
      end
      if timeout > 0
        EM.add_timer(timeout) do
          next if !@run_lock.locked?
          @run_lock.unlock
          send_sig('kill')
          $Logger.info "Sent kill"
          @state = 'timeout'
          @retcode = -1
          blk.call(self) if blk_given
          self.destroy
        end 
      end
      self
    end

    def send_sig(signal)
      j1 = @manager.create_job(@destkey)
      j1.run_job({:type => 'sig', :args => "#{@jobid} #{signal}"}) do
        :done
      end
    end

    def destroy
      [@stdin, @stdout, @stderr].each {|q| q.close; q.delete}
      begin
        @response_queue.unsubscribe
        @response_queue.delete
      rescue
      end
    end

    def subscribe_key(key,timeout = 0, &block)
      @response_queue = @channel.queue(key)
      @response_queue.bind(@exchange, :key => key)
      subscribe(@response_queue,timeout,&block)
    end

    def subscribe(queue, timeout = 0, &block)
      queue.subscribe do |h,d|
        begin
          res = block.call(h.headers,d)
        rescue Exception => e
          $Logger.error e.message
          $Logger.error e.backtrace
        end
      end
    end

    def prepare_header(jobheader)
      headers = {}.merge(jobheader)
      headers['id'] = @id
      headers['timestamp'] = Time.now.utc.to_i.to_s
      headers['msg_key'] = make_rand_str(8)
      headers
    end


    def publish(jobheader, fs, destkey, timeout = 0, &blk)
      data = Base64.encode64(fs)
      headers = prepare_header(jobheader)
      $Logger.debug "Running #{headers} on #{destkey}"
      @exchange.publish(data, {:headers => headers, :routing_key=>destkey})
      key = headers['msg_key']
      if(block_given?)
        subscribe_key(key,timeout,&blk)
      end
      key
    end
  end

  class RemoteRunner
    attr_reader :exchange, :channel, :id
    def initialize(host,id)
      @jobs = []
      @jobs_lock = Mutex.new
      @jobs_group = []
      @jobs_group_lock = Mutex.new
      @hosts = []
      @id = id
      @amqp_host = host
    end

    def start(&blk)
      AMQP.start(:host => @amqp_host) { 
        @channel = AMQP::Channel.new
        @exchange = @channel.direct(Exchange, {:durable => true})
        $Logger.info "Connected to #{@amqp_host}: #{@channel}"
        blk.call(self)
      }
    end

    def create_job(dest)
      RemoteJob.new(self,dest)
    end
  end

end
