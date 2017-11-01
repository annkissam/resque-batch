module Resque
  class Batch
    # A batch uses this file to store the state of each job.
    # As messages are received (through redis) they're used to update this data.
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
