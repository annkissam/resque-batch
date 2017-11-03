module Resque
  module Plugins
    class Batch
      # This class will be instantiated in a job and can be used to interact with the batch (by passing messages to a redis list)
      class WorkerJobInfo
        attr_reader :batch_id,
                    :job_id

        def initialize(batch_id, job_id)
          @batch_id = batch_id
          @job_id = job_id
        end

        def begin!
          redis.rpush(batch_key, Resque.encode(id: job_id, status: 'running'))
        end

        def finish!(success, msg)
          if success
            redis.rpush(batch_key, Resque.encode(id: job_id, status: 'success', msg: msg))
          else
            redis.rpush(batch_key, Resque.encode(id: job_id, status: 'failure', msg: msg))
          end
        end

        def exception!(exception)
          redis.rpush(batch_key, Resque.encode(id: job_id, status: 'exception', exception: exception))
        end

        def heartbeat!
          redis.set(heartbeat_key, "running")
          redis.expire(heartbeat_key, Resque::Plugins::Batch::JOB_HEARTBEAT_TTL)
        end

        private

        def redis
          Resque.redis
        end

        def batch_key
          "batch:#{batch_id}"
        end

        def heartbeat_key
          "batch:#{batch_id}:heartbeat:#{job_id}"
        end
      end
    end
  end
end
