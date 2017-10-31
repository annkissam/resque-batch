require "resque"
require "resque/batch/version"

require "resque/batch/batch_job_info"
require "resque/batch/job"
require "resque/batch/worker_job_info"

module Resque
  class Batch
    attr_reader :id,
                :batch_jobs

    def initialize(id)
      @id = id
      @batch_jobs = []
    end

    def enqueue(klass, *args)
      batch_jobs << Resque::Batch::BatchJobInfo.new(klass, *args)
    end

    def perform(&block)
      result = redis.multi do
        # TODO: This should be atomic and raise and error if it's not empty
        # Make sure the incoming message queue is clear
        redis.del(batch_key)

        batch_jobs.each_with_index do |bath_job, job_id|
          klass = bath_job.klass
          args = bath_job.args
          args = [id, job_id] + args
          Resque::Job.create(batch_queue, klass, *args)
        end
      end

      # TODO: Test the responst of multi
      # raise "Error" unless result[0] == "OK"

      block.call(batch_jobs, :init, nil)

      while(batch_jobs.any?(&:incomplete?)) do
        _queue, msg = redis.blpop(batch_key)

        decoded_msg = Resque.decode(msg)
        job_id = decoded_msg["id"].to_i
        batch_jobs[job_id].status = decoded_msg["status"]
        batch_jobs[job_id].msg = decoded_msg["msg"]
        batch_jobs[job_id].exception = decoded_msg["exception"]

        block.call(batch_jobs, :status, decoded_msg)
      end

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

  end
end
