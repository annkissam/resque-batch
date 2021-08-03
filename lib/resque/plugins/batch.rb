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
      # How frequently heartbeat messages should be sent, in seconds.
      JOB_HEARTBEAT = 45

      # How long a heartbeat message should persist, in seconds.
      # If a heartbeat message times out, the job sending the heartbeat has
      # an "arrhythmia" event and its worker may have died.
      JOB_HEARTBEAT_TTL = 120

      attr_reader :id,
                  :message_handler

      # Gets information for all jobs that have been enqueued in this batch.
      # Jobs are ordered in the order in which they were enqueued; the first
      # item in the array is the first job that was enqueued, and the final item
      # is the most recently enqueued job.
      #
      # @return [Array<BatchJobInfo>]
      attr_reader :batch_jobs

      # Creates a new batch object.
      #
      # @param id [String, Integer, NilClass]
      #   a unique ID to identify this batch.  If unspecified, an ID number will
      #   be generated automatically.
      # @param message_handler [Resque::Plugins::Batch::MessageHandler]
      #   a handler for messages sent to the batch
      def initialize(id: nil, message_handler: Resque::Plugins::Batch::MessageHandler.new)
        @id = id || get_id
        @message_handler = message_handler
        @batch_jobs = []
      end

      # Queues up a batch job to handle batch data.  The job class MUST include
      # the Resque::Plugins::Batch::Job mixin.
      #
      # @param klass [Class]
      #   the batch job class
      # @param args [Array]
      #   the batch data to be processed by the batch job
      def enqueue(klass, *args)
        batch_jobs << Resque::Plugins::Batch::BatchJobInfo.new(id, batch_jobs.count, klass, *args)
      end

      # Performs the batch processing.  This method is BLOCKING and will wait
      # until all batch jobs have completed before the method exits.
      #
      # @return [Boolean]
      #   true if all batch jobs completed successfully, false if any job
      #   failed or encountered an error
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
              # rubocop:disable Lint/SuppressedException
              rescue StandardError => _exception
                # NOTE: We still want to use the normal job messaging
              end
              # rubocop:enable Lint/SuppressedException
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

        # While any job is still pending or running (heartbeat timeouts are not
        # considered to be incomplete), periodically check for messages and
        # for heartbeat timeouts.
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

            # Note: we check every 45 seconds for heartbeat timeouts, but the
            # timeout period is 120 seconds.  We'll check 2 or 3 times for
            # timeouts before finally sending an arrhythmia message
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
