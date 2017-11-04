module Resque
  module Plugins
    class Batch
      module Job
        def self.included(base)
          base.extend ClassMethods
          base.class_eval do
            @queue = :batch
          end
        end

        module ClassMethods
          def perform(batch_id, job_id, *params)
            heartbeat_thread = nil

            worker_job_info = Resque::Plugins::Batch::WorkerJobInfo.new(batch_id, job_id)

            worker_job_info.heartbeat!

            heartbeat_thread = Thread.new do
              loop do
                sleep(Resque::Plugins::Batch::JOB_HEARTBEAT)
                worker_job_info.heartbeat!
              end
            end

            begin
              worker_job_info.begin!

              success, data = perform_work(*params)

              if success
                worker_job_info.success!(data)
              else
                worker_job_info.failure!(data)
              end
            rescue StandardError => exception
              worker_job_info.exception!(exception)

              # TODO: Is this correct?
              if Resque.inline
                raise exception
              end
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
