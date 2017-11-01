module Resque
  class Batch
    # A batch uses this file to store the state of each job.
    # As messages are received (through redis) they're used to update this data.
    class BatchJobInfo
      attr_reader :batch_id,
                  :job_id

      attr_reader :klass,
                  :args

      attr_accessor :status,
                    :msg,
                    :exception

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
