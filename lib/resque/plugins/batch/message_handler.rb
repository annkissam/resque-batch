module Resque
  module Plugins
    class Batch
      class MessageHandler
        attr_accessor :init_handler,
                      :idle_handler,
                      :exit_handler,
                      :status_handler
                      # :job_start_handler,
                      # :job_stop_handler,
                      # :job_info_handler,
                      # :job_exception_handler

        def initialize(options = {})
          @init_handler = options.fetch(:init, ->(_batch_jobs){byebug})
          @idle_handler = options.fetch(:idle, ->(_batch_jobs) { raise "IDLE: there appears to be no activity" })
          @exit_handler = options.fetch(:exit, ->(_batch_jobs){byebug})

          @status_handler = options.fetch(:status, ->(_batch_jobs, msg){byebug})

          # @job_start_handler = options.fetch(:job_start, ->(){})
          # @job_stop_handler = options.fetch(:job_stop, ->(){})
          # @job_info_handler = options.fetch(:job_info, ->(){})
          # @job_exception_handler = options.fetch(:job_exception, ->(){})
        end

        def send_init(batch)
          init_handler.call(batch.batch_jobs)
        end

        def send_idle(batch)
          idle_handler.call(batch.batch_jobs)
        end

        def send_exit(batch)
          exit_handler.call(batch.batch_jobs)
        end

        def receive_msg(batch, msg)
          status_handler.call(batch.batch_jobs, msg)
        end

        # def receive_job_msg(batch, msg)
        # end
      end
    end
  end
end
