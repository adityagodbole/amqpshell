dir = File.dirname(__FILE__)
require File.expand_path(dir + '/queueio.rb')
require File.expand_path(dir + '/utils.rb')

module AmqpShell
  # Node side classes
  # AmqpJob - Class representing a job running on a node
  # AmqpNode - Class providing features to implement a node
  class AmqpJob
    attr_reader :jobid
    attr_reader :stdin, :stdout, :stderr
    attr_reader :status
    def initialize(nodeid, jobid, inqueue , outqueue, errqueue)
      @nodeid = nodeid
      @jobid = jobid
      @stdin = @inqueue = QueueIO.new(inqueue,'r',@nodeid,@jobid)
      @stdout = @outqueue = QueueIO.new(outqueue,'w',@nodeid,@jobid)
      @stderr = @errqueue = QueueIO.new(errqueue,'w',@nodeid,@jobid)
      @status = 'init'
      @jobthread = nil
    end


    def run(fs, args, &block)
      fsdir = make_empty_temp_dir(make_rand_str(8))
      if !fs.empty?
        zipdir = make_empty_temp_dir(make_rand_str(8))
        infs = zipdir + "/infs.zip"
        outfs = zipdir + "/outfs.zip"
        File.open(infs,"wb") do |f|
          f.write(fs) 
        end
        unzip_file(infs,fsdir)
      end
      ret = ""
      @status = 'started'
      begin
        @jobthread = Thread.new { block.call(self, args, fsdir) }
        ret = @jobthread.join
      ensure
        @status = 'finished'
        ret ||= ""
        fsout = "nil"
        if !fs.empty?
          begin
            zip_dir(fsdir,outfs)
            fsout = File.open(outfs).read
            FileUtils.rm_rf(zipdir)
          rescue
            nil
          end
        end
        FileUtils.rm_rf(fsdir)
      end
      {'return' => ret.to_s, 'fs' => fsout}
    end

    def on_kill(&blk)
      @on_kill = blk
    end

    def kill!
      @on_kill.call
      @jobthread.kill
    end

    def on_sig(&blk)
      @on_sig = blk
    end

    def sig(sig)
      @on_sig.call(sig)
    end

  end

  #Class Job ends

  class AmqpNode

    def initialize(id, host)
      @jobs = []
      @jobs_lock = Mutex.new
      @id = id
      @host = host
      @header_skel = {'id' => @id}
      @handlers = []
      register_job_handler('ping') { |j, args, dir| j.stdout.send 'pong'; 0}
      register_job_handler('ps') { |j, args, dir| 
        @logger.info "Processing ps request"
        jobs = @jobs
        if !args.empty?
          argv = args.split
          jobs = @jobs.select{|j| argv.index(j['id'])}
        end
        j.stdout.send(jobs.map{|j| j['id'] + ":" + j['job'].status}.join("\n"))
        0
      }
      register_job_handler('kill') { |j, args, dir|
        argv = args.split
        jobid = argv[0]
        job = @jobs.find {|job| job['id'] == jobid}
        $Logger.info "Sending signal #{sig} to #{jobid}"
        job.kill! if job
        0
      }
      register_job_handler('sig') { |j, args, dir| 
        argv = args.split
        jobid = argv[0]
        sig = argv[1]
        job = @jobs.find {|job| job['id'] == jobid}
        $Logger.info "Sending signal #{sig} to #{jobid}"
        job['job'].sig(sig) if job
        0
      }
    end

    def start
      AMQP.start(:host => @host) { 
        @channel = AMQP::Channel.new
        @exchange = @channel.direct(Exchange, {:durable => true})
        $Logger.info "#{@id}: Waiting for request"
        @channel.queue(@id, :durable => true).bind(@exchange, :key => @id).subscribe do |h,m|
          begin
            header = h.headers
            run_job(header,m)
          rescue Exception => e
            msg = e.message.to_s + e.backtrace.to_s
            $Logger.error e.message
            $Logger.error e.backtrace.join("\n")
            @exchange.publish("nil", 
                              {:headers => @header_skel.merge({'jobid' => header['jobid'], 'return' => Base64.encode64(msg)}),
                                :routing_key => header['msg_key']}
                             )
          end
        end 
      }
    end

    def register_job_handler(type,&handler)
      @handlers << {'type' => type, 'handler' => handler}
    end

    def run_job(header,message)
      type = header['type']
      handler = @handlers.find {|h| h['type'] == header['type']} 
      handler = handler['handler']
      jobid = header['jobid']
      flags = header['flags']
      msg_key = header['msg_key']
      if !handler
        raise "InvalidJobType"
      end
      $Logger.debug @jobs.map{|j| j['id']}.join(",")
      if @jobs.find{|j| j['id'] == jobid}
        raise "DuplicateJobid #{jobid}"
      end
      fs = Base64.decode64(message)
      args = header['args'] || ""
      Thread.new do
        begin
          if flags and (flags | Unique)
            @jobs.each do |job|
              job['thread'].join
            end
          end
          inqueue = @channel.queue(jobid + "in")
          outqueue = @channel.queue(jobid + "out")
          errqueue = @channel.queue(jobid + "out")
          starttime = Time.now.utc.to_i.to_s
          job = AmqpJob.new(@id, jobid, inqueue, outqueue, errqueue)
          @jobs_lock.lock
          job_desc = {'id' => jobid, 'thread' => Thread.current, 'job' => job}
          @jobs << job_desc
          @jobs_lock.unlock
          endtime = Time.now.utc.to_i.to_s
          outheaders = {'jobid' => jobid, 'state' => job.status, 'type' => type, 'id' => @id}
          @exchange.publish("nil", {:headers => outheaders, :routing_key => msg_key})
          output = job.run(fs,args,&handler)
          outheaders = {'jobid' => jobid, 'state' => job.status, 'type' => type, 'id' => @id,
            'return' => output['return'],
            'startts' => starttime, 'endts' => endtime}
          @exchange.publish(Base64.encode64(output['fs']),
                            {:headers => outheaders, :routing_key => msg_key})
          @jobs_lock.lock
          @jobs.delete job_desc
          @jobs_lock.unlock
        rescue Exception => e
          $Logger.error e.message
          $Logger.error e.backtrace
        end
      end
    end
  end

end
