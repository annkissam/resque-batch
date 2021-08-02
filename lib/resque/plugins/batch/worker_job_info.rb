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
          redis.rpush(batch_key, Resque.encode(job_id: job_id, msg: 'begin'))
        end

        def success!(data)
          redis.rpush(batch_key, Resque.encode(job_id: job_id, msg: 'success', data: data))
        end

        def failure!(data)
          redis.rpush(batch_key, Resque.encode(job_id: job_id, msg: 'failure', data: data))
        end

        def exception!(exception)
          redis.rpush(batch_key, Resque.encode(job_id: job_id, msg: 'exception', data: {class: exception.class.name, message: exception.message, backtrace: exception.backtrace}))
        end

        # NOTE: This is the only message that the client should send
        def info!(data)
          redis.rpush(batch_key, Resque.encode(job_id: job_id, msg: 'info', data: data))
        end

        def heartbeat!
          # If the heartbeat key is still present, this will just refresh the
          # TTL to keep it alive for another 120 seconds
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
