require "resque"
require "resque/batch/version"

require "resque/batch/batch_job_info"
require "resque/batch/job"
require "resque/batch/worker_job_info"

module Resque
  class Batch
    attr_reader :id,
                :batch_jobs

    def initialize(id = nil)
      @id = id || get_id
      @batch_jobs = []
    end

    def enqueue(klass, *args)
      batch_jobs << Resque::Batch::BatchJobInfo.new(klass, *args)
    end

    def perform(&block)
      # Make sure the incoming message queue is clear
      if redis.llen(batch_key) > 0
        raise "redis list #{batch_key} is not empty"
      end

      result = redis.multi do
        batch_jobs.each_with_index do |bath_job, job_id|
          klass = bath_job.klass
          args = bath_job.args
          args = [id, job_id] + args
          Resque::Job.create(batch_queue, klass, *args)
        end
      end

      unless result.last == job_count
        raise "not all jobs were queued"
      end

      block.call(batch_jobs, :init, nil) if block

      while(batch_jobs.any?(&:incomplete?)) do
        msg = redis.lpop(batch_key)

        if msg
          decoded_msg = Resque.decode(msg)
          job_id = decoded_msg["id"].to_i
          batch_jobs[job_id].status = decoded_msg["status"]
          batch_jobs[job_id].msg = decoded_msg["msg"]
          batch_jobs[job_id].exception = decoded_msg["exception"]

          block.call(batch_jobs, :status, decoded_msg) if block
        else
          # TODO: How long should this sleep? Any feedback or timeout?
          sleep(1)
        end
      end

      block.call(batch_jobs, :exit, nil) if block

      # Cleanup
      redis.del(batch_key)

      batch_jobs.all?(&:success?)
    end

    def job_count
      batch_jobs.size
    end

  private

    def redis
      Resque.redis
    end

    def batch_queue
      "batch"
    end

    def batch_key
      "batch:#{id}"
    end

    # https://redis.io/commands/incr
    # An atomic counter
    # Used to identify the response list (batch_key)
    def get_id
      redis.incr("batch:id")
    end

  end
end
