module Resque
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
          worker_job_info = Resque::Batch::WorkerJobInfo.new(batch_id, job_id)

          begin
            worker_job_info.begin!

            success, msg = perform_work(*params)

            worker_job_info.finish!(success, msg)
          rescue StandardError => exception
            worker_job_info.exception!(exception)

            raise exception
          end
        end
      end
    end
  end
end
