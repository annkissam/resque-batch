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
          @init_handler = options.fetch(:init, ->(_batch){})
          @exit_handler = options.fetch(:exit, ->(_batch){})
          @idle_handler = options.fetch(:idle, ->(_batch, msg){})

          # @info_handler = options.fetch(:info, ->(_batch, msg){})

          @job_begin_handler = options.fetch(:job_begin, ->(_batch, _job_id){})
          @job_success_handler = options.fetch(:job_success, ->(_batch, _job_id, _data){})
          @job_failure_handler = options.fetch(:job_failure, ->(_batch, _job_id, _data){})
          @job_exception_handler = options.fetch(:job_exception, ->(_batch, _job_id, _data){})
          @job_info_handler = options.fetch(:job_info, ->(_batch, _job_id, _data){})
        end

        def send_message(batch, type, msg = {})
          case type
          when :init
            send_init(batch)
          when :exit
            send_exit(batch)
          when :idle
            send_idle(batch, msg)
          when :job
            send_job(batch, msg)
          else
            raise "unknown message type: #{type}"
          end
        end

        private

        def send_init(batch)
          init_handler.call(batch)
        end

        def send_exit(batch)
          exit_handler.call(batch)
        end

        def send_idle(batch, msg)
          idle_handler.call(batch, msg)
        end

        # def send_info(batch, msg)
        # end

        def send_job(batch, msg)
          job_id = msg["job_id"]

          case msg["msg"]
          when "begin"
            job_begin_handler.call(batch, job_id)
          when "success"
            job_success_handler.call(batch, job_id, msg["data"])
          when "failure"
            job_failure_handler.call(batch, job_id, msg["data"])
          when "exception"
            job_exception_handler.call(batch, job_id, msg["data"])
          when "info"
            job_info_handler.call(batch, job_id, msg["data"])
          else
            raise "unknown msg type: #{msg["msg"]}"
          end
        end

      end
    end
  end
end
