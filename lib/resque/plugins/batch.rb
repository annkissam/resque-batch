require 'resque/plugins/batch'

require "resque"
require "resque/plugins/batch/version"

require "resque/plugins/batch/batch_job_info"
require "resque/plugins/batch/job"
require "resque/plugins/batch/message_handler"
require "resque/plugins/batch/worker_job_info"

module Resque
  module Plugins
    class Batch
      JOB_HEARTBEAT = 45
      JOB_HEARTBEAT_TTL = 60

      attr_reader :id,
                  :batch_jobs

      def initialize(id: nil)
        @id = id || get_id
        @batch_jobs = []
      end

      def enqueue(klass, *args)
        batch_jobs << Resque::Plugins::Batch::BatchJobInfo.new(id, batch_jobs.count, klass, *args)
      end

      def perform(idle_callback_timeout = nil, &block)
        # Make sure the incoming message queue is clear
        if redis.llen(batch_key) > 0
          raise "redis list #{batch_key} is not empty"
        end

        result = redis.multi do
          batch_jobs.each_with_index do |batch_job, job_id|
            klass = batch_job.klass
            args = batch_job.args
            args = [id, job_id] + args
            Resque::Job.create(batch_queue, klass, *args)
          end
        end

        unless Resque.inline
          unless result.last == job_count
            raise "not all jobs were queued"
          end
        end

        block.call(batch_jobs, :init, nil) if block

        last_heartbeat_check = Time.now
        last_activity_check = Time.now

        while(batch_jobs.any?(&:incomplete?)) do
          msg = redis.lpop(batch_key)

          if msg
            decoded_msg = Resque.decode(msg)
            job_id = decoded_msg["id"].to_i
            batch_jobs[job_id].process_job_msg(decoded_msg)

            last_activity_check = Time.now

            block.call(batch_jobs, :status, decoded_msg) if block
          else
            # Reasons these may be no message
            # No Workers - check worker count
            # Workers are processing another batch - register batches, check status
            # A Job takes a long time - send a headbeat
            # A Job dies - send a heartbeat

            if Time.now - last_heartbeat_check > JOB_HEARTBEAT
              running_jobs = batch_jobs.select(&:running?)
              if running_jobs.any?(&:flatlined?)
                raise "a job died..."
              else
                last_heartbeat_check = Time.now
              end

              if running_jobs.any?
                last_activity_check = Time.now
              end
            end

            if idle_callback_timeout
              if Time.now - last_activity_check > idle_callback_timeout
                if block
                  block.call(batch_jobs, :idle, decoded_msg)
                else
                  raise "IDLE: there appears to be no activity"
                end
              end
            end

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
end
