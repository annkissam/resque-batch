module Resque
  module Plugins
    class Batch
      # A batch uses this file to store the state of each job.
      # As messages are received (through redis) they're used to update this data.
      class BatchJobInfo
        attr_reader :batch_id,
                    :job_id,
                    :klass,
                    :args

        attr_reader :status,
                    :result,
                    :exception,
                    :duration

        def initialize(batch_id, job_id, klass, *args)
          @batch_id = batch_id
          @job_id = job_id
          @klass = klass
          @args = args
          @status = 'pending'
        end

        def running?
          status == 'running'
        end

        def success?
          status == 'success'
        end

        def complete?
          ['success', 'failure', 'exception'].include?(status)
        end

        def incomplete?
          !complete?
        end

        def flatlined?
          redis.get(heartbeat_key) != "running"
        end

        # Process the msg sent from WorkerJobInfo
        def process_job_msg(job_msg)
          msg = job_msg["msg"]
          data = job_msg["data"]

          if status == 'pending' && msg == 'begin'
            @status = 'running'
            @start_time = Time.now
          elsif status == 'running' && (msg == 'success' || msg == 'failure')
            @status = msg
            @result = data
            @duration = Time.now - @start_time
          elsif status == 'running' && msg == 'exception'
            @status = 'exception'
            @exception = data
            @duration = Time.now - @start_time
          else
            raise "State machine Error #{job_msg}"
          end
        end

        private

        def redis
          Resque.redis
        end

        def heartbeat_key
          "batch:#{batch_id}:heartbeat:#{job_id}"
        end
      end
    end
  end
end
