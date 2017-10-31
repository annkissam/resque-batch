module Resque
  class Batch
    class BatchJobInfo
      attr_reader :klass,
                  :args

      attr_accessor :status,
                    :msg,
                    :exception

      def initialize(klass, *args)
        @klass = klass
        @args = args
        @status = 'pending'
      end

      def success?
        status == 'success'
      end

      def complete?
        ['success', 'failure', 'exception'].include?(status)
      end

      def incomplete?
        !complete?
      end
    end
  end
end
