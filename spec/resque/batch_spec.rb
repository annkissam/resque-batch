require "spec_helper"

class Job
  include Resque::Plugins::Batch::Job

  def self.perform_work(id, data = "test")
    @processed_jobs << [id, data]

    if data == "ERROR"
      return false, data
    elsif data == "EXCEPTION"
      raise "Unknown Exception"
    elsif data == "INFO"
      @worker_job_info.info!(test: "INFO")
      return true, data + "-success"
    else
      return true, data + "-success"
    end
  end

  # NOTE: This isn't threadsafe, so don't treat you tests as such...
  def self.processed_jobs
    @processed_jobs ||= []
  end

  def self.reset_jobs
    @processed_jobs = []
  end
end

class LongJob
  include Resque::Plugins::Batch::Job

  def self.perform_work
    sleep(60)

    return true, nil
  end
end

class Batch2QueueJob
  include Resque::Plugins::Batch::Job

  @queue = :batch_2

  def self.perform_work
    return true, nil
  end
end

# NOTE: Normally this will be async, but it's easier to test this way
def process_job(queue = :batch)
  worker = Resque::Worker.new(queue)
  worker.fork_per_job = false
  worker.work_one_job
end

RSpec.describe Resque::Plugins::Batch do
  before do
    Job.reset_jobs

    Resque::Stat.clear(:processed)
    Resque::Stat.clear(:failed)

    Resque.remove_queue(:batch)

    # The default
    Resque.inline = false
  end

  let(:message_handler) { Resque::Plugins::Batch::MessageHandler.new }

  it "has a version number" do
    expect(Resque::Plugins::Batch::VERSION).not_to be nil
  end

  # NOTE: Resque does not set Resque::Stat for inline
  [false, true].each do |inline|
    context "Resque.inline == #{inline}" do
      before do
        Resque.inline = inline
      end

      it "works (with a message_handler)" do
        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(Job, 11)
        batch.enqueue(Job, 12, "ERROR")
        batch.enqueue(Job, 13, "EXCEPTION")

        message_handler.init_handler = ->(_batch) do
          expect(batch.batch_jobs[0].status).to eq("pending")
          expect(batch.batch_jobs[0].batch_id).to eq(batch.id)
          expect(batch.batch_jobs[0].job_id).to eq(0)

          expect(batch.batch_jobs[1].status).to eq("pending")
          expect(batch.batch_jobs[1].batch_id).to eq(batch.id)
          expect(batch.batch_jobs[1].job_id).to eq(1)

          expect(batch.batch_jobs[2].status).to eq("pending")
          expect(batch.batch_jobs[2].batch_id).to eq(batch.id)
          expect(batch.batch_jobs[2].job_id).to eq(2)

          # Kickoff an initial Job
          process_job(:batch)
        end

        message_handler.job_begin_handler = ->(_batch, job_id) do
          if job_id == 0
            expect(batch.batch_jobs[0].status).to eq("running")
            expect(batch.batch_jobs[0].klass).to eq(Job)
            expect(batch.batch_jobs[0].args).to eq([11])
            expect(batch.batch_jobs[0].result).to eq(nil)
            expect(batch.batch_jobs[0].exception).to eq(nil)

          elsif job_id == 1
            expect(batch.batch_jobs[1].status).to eq("running")
            expect(batch.batch_jobs[1].klass).to eq(Job)
            expect(batch.batch_jobs[1].args).to eq([12, "ERROR"])
            expect(batch.batch_jobs[1].result).to eq(nil)
            expect(batch.batch_jobs[1].exception).to eq(nil)

          elsif job_id == 2
            expect(batch.batch_jobs[2].status).to eq("running")
            expect(batch.batch_jobs[2].klass).to eq(Job)
            expect(batch.batch_jobs[2].args).to eq([13, "EXCEPTION"])
            expect(batch.batch_jobs[2].result).to eq(nil)
            expect(batch.batch_jobs[2].exception).to eq(nil)

          else
            raise "SPEC FAILURE"
          end
        end

        message_handler.job_success_handler = ->(_batch, job_id, data) do
          if job_id == 0
            expect(data).to eq("test-success")

            expect(batch.batch_jobs[0].status).to eq("success")
            expect(batch.batch_jobs[0].klass).to eq(Job)
            expect(batch.batch_jobs[0].args).to eq([11])
            expect(batch.batch_jobs[0].result).to eq("test-success")
            expect(batch.batch_jobs[0].exception).to eq(nil)

            # Kickoff a second Job
            process_job(:batch)

          else
            raise "SPEC FAILURE"
          end
        end

        message_handler.job_failure_handler = ->(_batch, job_id, data) do
          if job_id == 1
            expect(data).to eq("ERROR")

            expect(batch.batch_jobs[1].status).to eq("failure")
            expect(batch.batch_jobs[1].klass).to eq(Job)
            expect(batch.batch_jobs[1].args).to eq([12, "ERROR"])
            expect(batch.batch_jobs[1].result).to eq("ERROR")
            expect(batch.batch_jobs[1].exception).to eq(nil)
            expect(batch.batch_jobs[1].duration).to be > 0

            # Kickoff a third Job
            process_job(:batch)

          else
            raise "SPEC FAILURE"
          end
        end

        message_handler.job_exception_handler = ->(_batch, job_id, data) do
          if job_id == 2
            expect(data.keys).to eq(["class", "message", "backtrace"])
            expect(data["class"]).to eq("RuntimeError")
            expect(data["message"]).to eq("Unknown Exception")

            expect(batch.batch_jobs[2].status).to eq("exception")
            expect(batch.batch_jobs[2].klass).to eq(Job)
            expect(batch.batch_jobs[2].args).to eq([13, "EXCEPTION"])
            expect(batch.batch_jobs[2].result).to eq(nil)
            expect(batch.batch_jobs[2].exception.keys).to eq(["class", "message", "backtrace"])
            expect(batch.batch_jobs[2].exception["class"]).to eq("RuntimeError")
            expect(batch.batch_jobs[2].exception["message"]).to eq("Unknown Exception")
            expect(batch.batch_jobs[2].duration).to be > 0

          else
            raise "SPEC FAILURE"
          end
        end

        message_handler.exit_handler = ->(_batch) do
          expect(batch.batch_jobs[0].status).to eq("success")
          expect(batch.batch_jobs[1].status).to eq("failure")
          expect(batch.batch_jobs[2].status).to eq("exception")
        end

        allow(message_handler.init_handler).to receive(:call).and_call_original
        allow(message_handler.job_begin_handler).to receive(:call).and_call_original
        allow(message_handler.job_success_handler).to receive(:call).and_call_original
        allow(message_handler.job_failure_handler).to receive(:call).and_call_original
        allow(message_handler.job_exception_handler).to receive(:call).and_call_original
        allow(message_handler.exit_handler).to receive(:call).and_call_original

        expect(Resque.size(:batch)).to eq(0)
        expect(Resque::Stat[:processed]).to eq(0)
        expect(Resque::Stat[:failed]).to eq(0)
        expect(Job.processed_jobs).to eq([
        ])

        result = batch.perform

        expect(result).to be_falsey

        expect(Resque.size(:batch)).to eq(0)
        if Resque.inline
          expect(Resque::Stat[:processed]).to eq(0)
          expect(Resque::Stat[:failed]).to eq(0)
        else
          expect(Resque::Stat[:processed]).to eq(3)
          expect(Resque::Stat[:failed]).to eq(1)
        end
        expect(Job.processed_jobs).to eq([
          [11, "test"],
          [12, "ERROR"],
          [13, "EXCEPTION"],
        ])

        expect(message_handler.init_handler).to have_received(:call)
        expect(message_handler.job_begin_handler).to have_received(:call).exactly(3).times
        expect(message_handler.job_success_handler).to have_received(:call)
        expect(message_handler.job_failure_handler).to have_received(:call)
        expect(message_handler.job_exception_handler).to have_received(:call)
        expect(message_handler.exit_handler).to have_received(:call)
      end

      it "works (without a message_handler)" do
        begin
          t = Thread.new {
            loop do
              if Resque.size(:batch) > 0
                process_job(:batch)
              else
                sleep(0.1)
              end
            end
          } unless Resque.inline

          batch = Resque::Plugins::Batch.new()
          batch.enqueue(Job, 11)
          batch.enqueue(Job, 12, "ERROR")
          batch.enqueue(Job, 13, "EXCEPTION")

          expect(Resque.size(:batch)).to eq(0)
          expect(Resque::Stat[:processed]).to eq(0)
          expect(Resque::Stat[:failed]).to eq(0)
          expect(Job.processed_jobs).to eq([
          ])

          result = batch.perform

          expect(result).to be_falsey

          expect(Resque.size(:batch)).to eq(0)
          if Resque.inline
            expect(Resque::Stat[:processed]).to eq(0)
            expect(Resque::Stat[:failed]).to eq(0)
          else
            expect(Resque::Stat[:processed]).to eq(3)
            expect(Resque::Stat[:failed]).to eq(1)
          end
          expect(Job.processed_jobs).to eq([
            [11, "test"],
            [12, "ERROR"],
            [13, "EXCEPTION"],
          ])
        ensure
          t.exit unless Resque.inline
        end
      end
    end
  end

  context "message_handler callbacks" do
    around(:each) do |example|
      t = Thread.new do
        loop do
          if Resque.size(:batch) > 0
            process_job(:batch)
          else
            sleep(0.1)
          end
        end
      end

      begin
        example.run
      ensure
        t.exit
      end
    end

    context "init_handler" do
      it "is called when processing begins" do
        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(Job, 12, "TEST")

        message_handler.init_handler = ->(_batch) do
          expect(batch.batch_jobs.size).to eq(1)
          expect(batch.batch_jobs[0].status).to eq("pending")
        end

        allow(message_handler.init_handler).to receive(:call).and_call_original

        result = batch.perform

        expect(result).to be_truthy

        expect(batch.batch_jobs[0].status).to eq("success")
        expect(batch.batch_jobs[0].klass).to eq(Job)
        expect(batch.batch_jobs[0].args).to eq([12, "TEST"])
        expect(batch.batch_jobs[0].result).to eq("TEST-success")
        expect(batch.batch_jobs[0].exception).to eq(nil)
        expect(batch.batch_jobs[0].duration).to be > 0

        expect(message_handler.init_handler).to have_received(:call)
      end
    end

    context "exit_handler" do
      it "is called when processing completes" do
        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(Job, 12, "TEST")

        message_handler.exit_handler = ->(_batch) do
          expect(batch.batch_jobs.size).to eq(1)
          expect(batch.batch_jobs[0].status).to eq("success")
        end

        allow(message_handler.exit_handler).to receive(:call).and_call_original

        result = batch.perform

        expect(result).to be_truthy

        expect(batch.batch_jobs[0].status).to eq("success")
        expect(batch.batch_jobs[0].klass).to eq(Job)
        expect(batch.batch_jobs[0].args).to eq([12, "TEST"])
        expect(batch.batch_jobs[0].result).to eq("TEST-success")
        expect(batch.batch_jobs[0].exception).to eq(nil)
        expect(batch.batch_jobs[0].duration).to be > 0

        expect(message_handler.exit_handler).to have_received(:call)
      end
    end

    context "idle_handler" do
      it "is called when processing is idle" do
        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(Job, 12, "TEST")

        message_handler.idle_handler = ->(_batch_jobs, msg) do
          expect(msg.keys).to eq([:duration])
          expect(msg[:duration]).to be > 0
        end

        allow(message_handler.idle_handler).to receive(:call).and_call_original

        result = batch.perform

        expect(result).to be_truthy

        expect(message_handler.idle_handler).to have_received(:call)
      end
    end

    context "job_begin_handler" do
      it "is called when a job begins" do
        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(Job, 12, "TEST")

        message_handler.job_begin_handler = ->(_batch, job_id) do
          if job_id == 0
            expect(batch.batch_jobs[0].status).to eq("running")
            expect(batch.batch_jobs[0].klass).to eq(Job)
            expect(batch.batch_jobs[0].args).to eq([12, "TEST"])
            expect(batch.batch_jobs[0].result).to eq(nil)
            expect(batch.batch_jobs[0].exception).to eq(nil)
            expect(batch.batch_jobs[0].duration).to eq(nil)

          else
            raise "SPEC FAILURE"
          end
        end

        allow(message_handler.job_begin_handler).to receive(:call).and_call_original

        result = batch.perform

        expect(result).to be_truthy

        expect(batch.batch_jobs[0].status).to eq("success")
        expect(batch.batch_jobs[0].klass).to eq(Job)
        expect(batch.batch_jobs[0].args).to eq([12, "TEST"])
        expect(batch.batch_jobs[0].result).to eq("TEST-success")
        expect(batch.batch_jobs[0].exception).to eq(nil)
        expect(batch.batch_jobs[0].duration).to be > 0

        expect(message_handler.job_begin_handler).to have_received(:call)
      end
    end

    context "job_success_handler" do
      it "is called when a job succeeds" do
        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(Job, 12, "TEST")

        message_handler.job_success_handler = ->(_batch, job_id, data) do
          if job_id == 0
            expect(data).to eq("TEST-success")

            expect(batch.batch_jobs[0].status).to eq("success")
            expect(batch.batch_jobs[0].klass).to eq(Job)
            expect(batch.batch_jobs[0].args).to eq([12, "TEST"])
            expect(batch.batch_jobs[0].result).to eq("TEST-success")
            expect(batch.batch_jobs[0].exception).to eq(nil)
            expect(batch.batch_jobs[0].duration).to be > 0

          else
            raise "SPEC FAILURE"
          end
        end

        allow(message_handler.job_success_handler).to receive(:call).and_call_original

        result = batch.perform

        expect(result).to be_truthy

        expect(batch.batch_jobs[0].status).to eq("success")
        expect(batch.batch_jobs[0].klass).to eq(Job)
        expect(batch.batch_jobs[0].args).to eq([12, "TEST"])
        expect(batch.batch_jobs[0].result).to eq("TEST-success")
        expect(batch.batch_jobs[0].exception).to eq(nil)
        expect(batch.batch_jobs[0].duration).to be > 0

        expect(message_handler.job_success_handler).to have_received(:call)
      end
    end

    context "job_failure_handler" do
      it "is called when a job fails" do
        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(Job, 12, "ERROR")

        message_handler.job_failure_handler = ->(_batch, job_id, data) do
          if job_id == 0
            expect(data).to eq("ERROR")

            expect(batch.batch_jobs[0].status).to eq("failure")
            expect(batch.batch_jobs[0].klass).to eq(Job)
            expect(batch.batch_jobs[0].args).to eq([12, "ERROR"])
            expect(batch.batch_jobs[0].result).to eq("ERROR")
            expect(batch.batch_jobs[0].exception).to eq(nil)
            expect(batch.batch_jobs[0].duration).to be > 0

          else
            raise "SPEC FAILURE"
          end
        end

        allow(message_handler.job_failure_handler).to receive(:call).and_call_original

        result = batch.perform

        expect(result).to be_falsey

        expect(batch.batch_jobs[0].status).to eq("failure")
        expect(batch.batch_jobs[0].klass).to eq(Job)
        expect(batch.batch_jobs[0].args).to eq([12, "ERROR"])
        expect(batch.batch_jobs[0].result).to eq("ERROR")
        expect(batch.batch_jobs[0].exception).to eq(nil)
        expect(batch.batch_jobs[0].duration).to be > 0

        expect(message_handler.job_failure_handler).to have_received(:call)
      end
    end

    context "job_exception_handler" do
      it "is called when a job raises an exception" do
        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(Job, 12, "EXCEPTION")

        message_handler.job_exception_handler = ->(_batch, job_id, data) do
          if job_id == 0
            expect(data.keys).to eq(["class", "message", "backtrace"])
            expect(data["class"]).to eq("RuntimeError")
            expect(data["message"]).to eq("Unknown Exception")

            expect(batch.batch_jobs[0].status).to eq("exception")
            expect(batch.batch_jobs[0].klass).to eq(Job)
            expect(batch.batch_jobs[0].args).to eq([12, "EXCEPTION"])
            expect(batch.batch_jobs[0].result).to eq(nil)
            expect(batch.batch_jobs[0].exception.keys).to eq(["class", "message", "backtrace"])
            expect(batch.batch_jobs[0].exception["class"]).to eq("RuntimeError")
            expect(batch.batch_jobs[0].exception["message"]).to eq("Unknown Exception")
            expect(batch.batch_jobs[0].duration).to be > 0

          else
            raise "SPEC FAILURE"
          end
        end

        allow(message_handler.job_exception_handler).to receive(:call).and_call_original

        result = batch.perform

        expect(result).to be_falsey

        expect(batch.batch_jobs[0].status).to eq("exception")
        expect(batch.batch_jobs[0].klass).to eq(Job)
        expect(batch.batch_jobs[0].args).to eq([12, "EXCEPTION"])
        expect(batch.batch_jobs[0].result).to eq(nil)
        expect(batch.batch_jobs[0].exception.keys).to eq(["class", "message", "backtrace"])
        expect(batch.batch_jobs[0].exception["class"]).to eq("RuntimeError")
        expect(batch.batch_jobs[0].exception["message"]).to eq("Unknown Exception")
        expect(batch.batch_jobs[0].duration).to be > 0

        expect(message_handler.job_exception_handler).to have_received(:call)
      end
    end

    context "job_info_handler" do
      it "is called when a job sends additional info" do
        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(Job, 12, "INFO")

        message_handler.job_info_handler = ->(_batch, job_id, data) do
          if job_id == 0
            expect(data).to eq("test" => "INFO")

            expect(batch.batch_jobs[0].status).to eq("running")
            expect(batch.batch_jobs[0].klass).to eq(Job)
            expect(batch.batch_jobs[0].args).to eq([12, "INFO"])
            expect(batch.batch_jobs[0].result).to eq(nil)
            expect(batch.batch_jobs[0].exception).to eq(nil)
            expect(batch.batch_jobs[0].duration).to eq(nil)

          else
            raise "SPEC FAILURE"
          end
        end

        allow(message_handler.job_info_handler).to receive(:call).and_call_original

        result = batch.perform

        expect(result).to be_truthy

        expect(batch.batch_jobs[0].status).to eq("success")
        expect(batch.batch_jobs[0].klass).to eq(Job)
        expect(batch.batch_jobs[0].args).to eq([12, "INFO"])
        expect(batch.batch_jobs[0].result).to eq("INFO-success")
        expect(batch.batch_jobs[0].exception).to eq(nil)
        expect(batch.batch_jobs[0].duration).to be > 0

        expect(message_handler.job_info_handler).to have_received(:call)
      end
    end
  end

  context "heartbeat" do
    before do
      stub_const("Resque::Plugins::Batch::JOB_HEARTBEAT", 1)
      stub_const("Resque::Plugins::Batch::JOB_HEARTBEAT_TTL", 2)

      # This won't work w/ inline...
      Resque.inline = false
    end

    it "raises 'a job died' exception" do
      begin
        t = Thread.new do
          loop do
            if Resque.size(:batch) > 0
              process_job(:batch)
            else
              sleep(0.1)
            end
          end
        end

        batch = Resque::Plugins::Batch.new(message_handler: message_handler)
        batch.enqueue(LongJob)

        message_handler.job_begin_handler = ->(_batch, job_id) do
          if job_id == 0
            t.exit

          else
            raise "SPEC FAILURE"
          end
        end

        expect {
          batch.perform
        }.to raise_error(StandardError, "a job died...")

        # TODO: Add a flatline job message

      ensure
        t.exit
      end
    end
  end

  context "Different Queue" do
    before do
      Resque.inline = false
    end

    around(:each) do |example|
      t = Thread.new do
        loop do
          if Resque.size(:batch_2) > 0
            process_job(:batch_2)
          else
            sleep(0.1)
          end
        end
      end

      begin
        example.run
      ensure
        t.exit
      end
    end

    it "works (without a message_handler)" do
      batch = Resque::Plugins::Batch.new()
      batch.enqueue(Batch2QueueJob)
      batch.enqueue(Batch2QueueJob)
      batch.enqueue(Batch2QueueJob)

      expect(Resque.size(:batch_2)).to eq(0)
      expect(Resque::Stat[:processed]).to eq(0)
      expect(Resque::Stat[:failed]).to eq(0)
      expect(Job.processed_jobs).to eq([
      ])

      result = batch.perform

      expect(result).to be_truthy

      expect(Resque.size(:batch_2)).to eq(0)
      expect(Resque::Stat[:processed]).to eq(3)
      expect(Resque::Stat[:failed]).to eq(0)
    end
  end
end
