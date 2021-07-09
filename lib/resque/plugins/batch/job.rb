module Resque
  module Plugins
    class Batch
      # A mixin for supporting processing of a batch of work.
      #
      # Any class that includes this mixin MUST have a public class-level
      # `perform_work` method that takes the batch data, if any, as arguments:
      #
      # @example
      #   def self.perform_work # no batch data is passed in from the top level
      #   def self.perform_work(user_id) # processes data for a single user
      #   def self.perform_work(user_ids) # processes data for a group of users
      #
      # @example
      #   # Class for processing a subset of the data - a single batch of work.
      #   class ProcessBatchData
      #     include Resque::Plugins::Batch::Job
      #
      #     def self.perform_work(batch_data)
      #       # implementation
      #     end
      #   end
      #
      #   # Top-level processing class that gets queued up to Resque and divides
      #   # the data into batches, which get queued up to be processed by the
      #   # batch processing class
      #   class ProcessDataJob
      #     def self.perform
      #       batch = Resque::Plugins::Batch.new
      #
      #       get_data.in_batches.each do |batch_data|
      #         batch.enqueue(ProcessBatchData, batch_data)
      #       end
      #
      #       batch.perform
      #     end
      #   end
      module Job
        def self.included(base)
          base.extend ClassMethods
          base.class_eval do
          end
        end

        module ClassMethods
          def perform(batch_id, job_id, *params)
            heartbeat_thread = nil

            @worker_job_info = Resque::Plugins::Batch::WorkerJobInfo.new(batch_id, job_id)

            @worker_job_info.heartbeat!

            heartbeat_thread = Thread.new do
              loop do
                sleep(Resque::Plugins::Batch::JOB_HEARTBEAT)
                @worker_job_info.heartbeat!
              end
            end

            begin
              @worker_job_info.begin!

              success, data = perform_work(*params)

              if success
                @worker_job_info.success!(data)
              else
                @worker_job_info.failure!(data)
              end
            rescue StandardError => exception
              @worker_job_info.exception!(exception)
              raise exception
            end
          ensure
            if heartbeat_thread
              heartbeat_thread.exit
            end
          end
        end
      end
    end
  end
end
