module Resque
  module Plugins
    class Batch
      # NOTE: This is the default Message handler. It takes lambda to handle each message type.
      # It could be replaced with any other class that responds to send_message(batch, type, msg = {})

      # idle_duration: Set this to a number
      class MessageHandler
        attr_accessor :init_handler,
                      :exit_handler,
                      :idle_handler,
                      # :info_handler,
                      :job_begin_handler,
                      :job_success_handler,
                      :job_failure_handler,
                      :job_exception_handler,
                      :job_info_handler

        def initialize(options = {})
          @init_handler = options.fetch(:init, ->(_batch_jobs){})
          @exit_handler = options.fetch(:exit, ->(_batch_jobs){})
          @idle_handler = options.fetch(:idle, ->(_batch_jobs, msg){})

          # @info_handler = options.fetch(:info, ->(_batch_jobs, msg){})

          @job_begin_handler = options.fetch(:job_begin, ->(_batch_jobs, _job_id){})
          @job_success_handler = options.fetch(:job_success, ->(_batch_jobs, _job_id, _data){})
          @job_failure_handler = options.fetch(:job_failure, ->(_batch_jobs, _job_id, _data){})
          @job_exception_handler = options.fetch(:job_exception, ->(_batch_jobs, _job_id, _data){})
          @job_info_handler = options.fetch(:job_info, ->(_batch_jobs, _job_id, _data){})
        end

        def send_message(batch, type, msg = {})
          case type
          when :init
            send_init(batch.batch_jobs)
          when :exit
            send_exit(batch.batch_jobs)
          when :idle
            send_idle(batch.batch_jobs, msg)
          when :job
            send_job(batch.batch_jobs, msg)
          else
            raise "unknown message type: #{type}"
          end
        end

        private

        def send_init(batch_jobs)
          init_handler.call(batch_jobs)
        end

        def send_exit(batch_jobs)
          exit_handler.call(batch_jobs)
        end

        def send_idle(batch_jobs, msg)
          idle_handler.call(batch_jobs, msg)
        end

        # def send_info(batch_jobs, msg)
        # end

        def send_job(batch_jobs, msg)
          job_id = msg["job_id"]

          case msg["msg"]
          when "begin"
            job_begin_handler.call(batch_jobs, job_id)
          when "success"
            job_success_handler.call(batch_jobs, job_id, msg["data"])
          when "failure"
            job_failure_handler.call(batch_jobs, job_id, msg["data"])
          when "exception"
            job_exception_handler.call(batch_jobs, job_id, msg["data"])
          when "info"
            job_info_handler.call(batch_jobs, job_id, msg["data"])
          else
            raise "unknown msg type: #{msg["msg"]}"
          end
        end

      end
    end
  end
end
