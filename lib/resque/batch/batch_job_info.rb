module Resque
  class Batch
    # A batch uses this file to store the state of each job.
    # As messages are received (through redis) they're used to update this data.
    class BatchJobInfo
      attr_reader :batch_id,
                  :job_id,
                  :klass,
                  :args

      attr_reader :status,
                  :msg,
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
        job_msg_status = job_msg["status"]
        job_msg_msg = job_msg["msg"]
        job_msg_exception = job_msg["exception"]

        if status == 'pending' && job_msg_status == 'running'
          @status = job_msg_status
          @start_time = Time.now
        elsif status == 'running' && (job_msg_status == 'success' || job_msg_status == 'failure')
          @status = job_msg_status
          @msg = job_msg_msg
          @duration = Time.now - @start_time
        elsif status == 'running' && job_msg_status == 'exception'
          @status = job_msg_status
          @exception = job_msg_exception
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
