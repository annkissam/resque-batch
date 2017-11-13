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
      JOB_HEARTBEAT_TTL = 120

      attr_reader :id,
                  :message_handler,
                  :batch_jobs

      def initialize(id: nil, message_handler: Resque::Plugins::Batch::MessageHandler.new)
        @id = id || get_id
        @message_handler = message_handler
        @batch_jobs = []
      end

      def enqueue(klass, *args)
        batch_jobs << Resque::Plugins::Batch::BatchJobInfo.new(id, batch_jobs.count, klass, *args)
      end

      def perform
        # Make sure the incoming message queue is clear
        if redis.llen(batch_key) > 0
          raise "redis list #{batch_key} is not empty"
        end

        result = redis.multi do
          batch_jobs.each_with_index do |batch_job, job_id|
            klass = batch_job.klass
            queue = Resque.queue_from_class(klass) || batch_queue
            args = batch_job.args
            args = [id, job_id] + args

            if Resque.inline
              begin
                Resque::Job.create(queue, klass, *args)
              rescue StandardError => exception
                # NOTE: We still want to use the normal job messaging
              end
            else
              Resque::Job.create(queue, klass, *args)
            end
          end
        end

        unless Resque.inline
          unless result.last == job_count
            raise "not all jobs were queued"
          end
        end

        message_handler.send_message(self, :init)

        last_heartbeat_check = Time.now
        last_activity_check = Time.now

        while(batch_jobs.any?(&:incomplete?)) do
          msg = redis.lpop(batch_key)

          if msg
            decoded_msg = Resque.decode(msg)

            if decoded_msg["job_id"]
              job_id = decoded_msg["job_id"]
              batch_jobs[job_id].process_job_msg(decoded_msg)
            end

            last_activity_check = Time.now

            message_handler.send_message(self, :job, decoded_msg)
          else
            # Reasons there may be no message
            # No Workers - check worker count
            # Workers are processing another batch - register batches, check status
            # A Job takes a long time - send a heartbeat
            # A Job dies - send a heartbeat

            if Time.now - last_heartbeat_check > JOB_HEARTBEAT
              running_jobs = batch_jobs.select(&:running?)

              if running_jobs.any?(&:heartbeat_running?)
                last_activity_check = Time.now
              end

              last_heartbeat_check = Time.now

              running_jobs.reject(&:heartbeat_running?).each do |batch_job|
                decoded_msg = {"job_id" => batch_job.job_id, "msg" => "arrhythmia"}
                batch_jobs[job_id].process_job_msg(decoded_msg)
                message_handler.send_message(self, :job, decoded_msg)
              end
            end

            idle_duration = Time.now - last_activity_check
            message_handler.send_message(self, :idle, {duration: idle_duration})

            sleep(1)
          end
        end

        message_handler.send_message(self, :exit)

        batch_jobs.all?(&:success?)
      ensure
        # Cleanup
        redis.del(batch_key)
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
