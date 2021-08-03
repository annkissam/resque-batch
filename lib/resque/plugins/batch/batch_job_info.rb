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

        # Checks if the job is still running
        def running?
          status == 'running'
        end

        # Checks if the job successfully completed processing.
        def success?
          status == 'success'
        end

        # Checks if the job has completed processing, successfully or otherwise.
        def complete?
          %w[success failure exception].include?(status)
        end

        # Checks if the job has not finished processing.
        # This is similar to #complete?, EXCEPT this also accounts for jobs
        # that have had timed out heartbeat checks; a job that has had its
        # heartbeat time out will be neither #complete? nor #incomplete?
        def incomplete?
          !complete? && status != 'unknown'
        end

        # Checks if the job's heartbeat is still live.  If a heartbeat isn't set
        # again before the timeout period expires (which defaults to 2 minutes -
        # see Batch::JOB_HEARTBEAT_TTL), then the job will receive an arrhythmia
        # message and have its status set to 'unknown'.  If all other batch jobs
        # complete or have arrhythmias before the job sends another heartbeat,
        # it will be considered dead and batch processing will finish.
        def heartbeat_running?
          redis.get(heartbeat_key) == "running"
        end

        # Process the msg sent from WorkerJobInfo
        def process_job_msg(job_msg)
          msg = job_msg["msg"]
          data = job_msg["data"]

          if status == 'pending' && msg == 'begin'
            @status = 'running'
            @start_time = Time.now
          elsif (status == 'running' || status == 'unknown') && (msg == 'success' || msg == 'failure')
            @status = msg
            @result = data
            @duration = Time.now - @start_time
          elsif (status == 'running' || status == 'unknown') && msg == 'exception'
            @status = 'exception'
            @exception = data
            @duration = Time.now - @start_time
          elsif msg == "info"
            # Ignore client defined messages
            true
          elsif msg == "arrhythmia"
            @status = 'unknown'
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
