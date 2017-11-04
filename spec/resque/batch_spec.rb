require "spec_helper"

class Job
  include Resque::Plugins::Batch::Job

  def self.perform_work(id, data = "test")
    @processed_jobs << [id, data]

    if data == "ERROR"
      return false, data
    elsif data == "EXCEPTION"
      raise "Unknown Exception"
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

# Note: This delegate to the message handler, storing calls in a 'log' array
# It's uses in tests to make sure the correct handlers are called
class LoggingMessageHander
  attr_reader :message_handler,
              :log

  def initialize(message_handler: Resque::Plugins::Batch::MessageHandler.new)
    @message_handler = message_handler
    @log = []
  end

  # NOTE: We don't need to store the batch
  def send_message(batch, type, msg = {})
    @log << [type, msg]
    message_handler.send_message(batch, type, msg)
  end
end

# NOTE: Normally this will be async, but it's easier to test this way
def process_job(queue = :batch)
  worker = Resque::Worker.new(:batch)
  worker.fork_per_job = false
  worker.work_one_job
end

RSpec.describe Resque::Plugins::Batch do
  before do
    Job.reset_jobs

    Resque::Stat.clear(:processed)
    Resque::Stat.clear(:failed)

    Resque.remove_queue(:batch)
  end

  it "has a version number" do
    expect(Resque::Plugins::Batch::VERSION).not_to be nil
  end

  context "Resque.inline == false" do
    before do
      Resque.inline = false
    end

    it "works (with a message_handler)" do
      message_handler = Resque::Plugins::Batch::MessageHandler.new
      logging_message_handler = LoggingMessageHander.new(message_handler: message_handler)

      batch = Resque::Plugins::Batch.new(message_handler: logging_message_handler)
      batch.enqueue(Job, 11)
      batch.enqueue(Job, 12, "test2")

      expect(Resque.size(:batch)).to eq(0)
      expect(Resque::Stat[:processed]).to eq(0)
      expect(Resque::Stat[:failed]).to eq(0)
      expect(Job.processed_jobs).to eq([
      ])

      message_handler.init_handler = ->(batch_jobs) do
        expect(batch_jobs[0].status).to eq("pending")
        expect(batch_jobs[0].batch_id).to eq(batch.id)
        expect(batch_jobs[0].job_id).to eq(0)

        expect(batch_jobs[1].status).to eq("pending")
        expect(batch_jobs[1].batch_id).to eq(batch.id)
        expect(batch_jobs[1].job_id).to eq(1)

        # Kickoff an initial Job
        process_job(:batch)
      end

      message_handler.job_handler = ->(batch_jobs, job_id, job_msg) do
        msg = job_msg["msg"]
        data = job_msg["data"]

        if job_id == 0 && msg == 'begin'
          expect(data).to eq(nil)

          expect(batch_jobs[0].status).to eq("running")
          expect(batch_jobs[0].klass).to eq(Job)
          expect(batch_jobs[0].args).to eq([11])
          expect(batch_jobs[0].result).to eq(nil)
          expect(batch_jobs[0].exception).to eq(nil)

        elsif job_id == 0 && msg == 'success'
          expect(data).to eq("test-success")

          expect(batch_jobs[0].status).to eq("success")
          expect(batch_jobs[0].klass).to eq(Job)
          expect(batch_jobs[0].args).to eq([11])
          expect(batch_jobs[0].result).to eq("test-success")
          expect(batch_jobs[0].exception).to eq(nil)

          # Kickoff a second Job
          process_job(:batch)

        elsif job_id == 1 && msg == 'begin'
          expect(data).to eq(nil)

          expect(batch_jobs[1].status).to eq("running")
          expect(batch_jobs[1].klass).to eq(Job)
          expect(batch_jobs[1].args).to eq([12, "test2"])
          expect(batch_jobs[1].result).to eq(nil)
          expect(batch_jobs[1].exception).to eq(nil)

        elsif job_id == 1 && msg == 'success'
          expect(data).to eq("test2-success")

          expect(batch_jobs[1].status).to eq("success")
          expect(batch_jobs[1].klass).to eq(Job)
          expect(batch_jobs[1].args).to eq([12, "test2"])
          expect(batch_jobs[1].result).to eq("test2-success")
          expect(batch_jobs[1].exception).to eq(nil)

        else
          raise "SPEC FAILURE"
        end
      end

      message_handler.exit_handler = ->(batch_jobs) do
        expect(batch_jobs[0].status).to eq("success")
        expect(batch_jobs[1].status).to eq("success")
      end

      result = batch.perform

      expect(result).to be_truthy

      expect(Resque.size(:batch)).to eq(0)
      expect(Resque::Stat[:processed]).to eq(2)
      expect(Resque::Stat[:failed]).to eq(0)
      expect(Job.processed_jobs).to eq([
        [11, "test"],
        [12, "test2"],
      ])

      expect(logging_message_handler.log).to eq([
        [:init, {}],
        [:job, {"job_id" => 0, "msg" => "begin"}],
        [:job, {"job_id" => 0, "msg" => "success", "data" => "test-success"}],
        [:job, {"job_id" => 1, "msg" => "begin"}],
        [:job, {"job_id" => 1, "msg" => "success", "data" => "test2-success"}],
        [:exit, {}]
      ])
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
        }

        batch = Resque::Plugins::Batch.new()
        batch.enqueue(Job, 11)
        batch.enqueue(Job, 12, "test2")

        expect(Resque.size(:batch)).to eq(0)
        expect(Resque::Stat[:processed]).to eq(0)
        expect(Resque::Stat[:failed]).to eq(0)
        expect(Job.processed_jobs).to eq([
        ])

        result = batch.perform

        expect(result).to be_truthy

        expect(Resque.size(:batch)).to eq(0)
        expect(Resque::Stat[:processed]).to eq(2)
        expect(Resque::Stat[:failed]).to eq(0)
        expect(Job.processed_jobs).to eq([
          [11, "test"],
          [12, "test2"],
        ])
      ensure
        t.exit
      end
    end
  end

  # NOTE: Resque does not set Resque::Stat for inline
  # context "Resque.inline == true" do
  #   before do
  #     Resque.inline = true
  #   end

  #   after do
  #     Resque.inline = false
  #   end

  #   it "works (with a message_handler)" do
  #     batch = Resque::Plugins::Batch.new()
  #     batch.enqueue(Job, 11)
  #     batch.enqueue(Job, 12, "test2")

  #     expect(Resque.size(:batch)).to eq(0)
  #     expect(Job.processed_jobs).to eq([
  #     ])

  #     batch.message_handler.init_handler = ->(batch_jobs) do
  #       expect(batch_jobs[0].status).to eq("pending")
  #       expect(batch_jobs[0].batch_id).to eq(batch.id)
  #       expect(batch_jobs[0].job_id).to eq(0)

  #       expect(batch_jobs[1].status).to eq("pending")
  #       expect(batch_jobs[1].batch_id).to eq(batch.id)
  #       expect(batch_jobs[1].job_id).to eq(1)

  #       # Kickoff an initial Job
  #       process_job(:batch)
  #     end

  #     result = batch.perform do |batch_jobs, msg, data|
  #       case msg
  #       when :status
  #         id = data["id"]
  #         status = data["status"]

  #         if id == 0 && status == 'running'
  #           expect(batch_jobs[0].status).to eq("running")
  #           expect(batch_jobs[0].klass).to eq(Job)
  #           expect(batch_jobs[0].args).to eq([11])
  #           expect(batch_jobs[0].msg).to eq(nil)
  #           expect(batch_jobs[0].exception).to eq(nil)

  #         elsif id == 0 && status == 'success'
  #           expect(batch_jobs[0].status).to eq("success")
  #           expect(batch_jobs[0].klass).to eq(Job)
  #           expect(batch_jobs[0].args).to eq([11])
  #           expect(batch_jobs[0].msg).to eq("test-success")
  #           expect(batch_jobs[0].exception).to eq(nil)

  #           # Kickoff a second Job
  #           process_job(:batch)

  #         elsif id == 1 && status == 'running'
  #           expect(batch_jobs[1].status).to eq("running")
  #           expect(batch_jobs[1].klass).to eq(Job)
  #           expect(batch_jobs[1].args).to eq([12, "test2"])
  #           expect(batch_jobs[1].msg).to eq(nil)
  #           expect(batch_jobs[1].exception).to eq(nil)

  #         elsif id == 1 && status == 'success'
  #           expect(batch_jobs[1].status).to eq("success")
  #           expect(batch_jobs[1].klass).to eq(Job)
  #           expect(batch_jobs[1].args).to eq([12, "test2"])
  #           expect(batch_jobs[1].msg).to eq("test2-success")
  #           expect(batch_jobs[1].exception).to eq(nil)

  #         else
  #           raise "SPEC FAILURE"
  #         end
  #       when :exit
  #         expect(batch_jobs[0].status).to eq("success")
  #         expect(batch_jobs[1].status).to eq("success")
  #       else
  #         raise "Unknown message #{msg}"
  #       end
  #     end

  #     expect(result).to be_truthy

  #     expect(Resque.size(:batch)).to eq(0)
  #     expect(Job.processed_jobs).to eq([
  #       [11, "test"],
  #       [12, "test2"],
  #     ])
  #   end

  #   it "works (without a message_handler)" do
  #     begin
  #       t = Thread.new {
  #         loop do
  #           if Resque.size(:batch) > 0
  #             process_job(:batch)
  #           else
  #             sleep(0.1)
  #           end
  #         end
  #       }

  #       batch = Resque::Plugins::Batch.new()
  #       batch.enqueue(Job, 11)
  #       batch.enqueue(Job, 12, "test2")

  #       expect(Resque.size(:batch)).to eq(0)
  #       expect(Job.processed_jobs).to eq([
  #       ])

  #       result = batch.perform

  #       expect(result).to be_truthy

  #       expect(Resque.size(:batch)).to eq(0)
  #       expect(Job.processed_jobs).to eq([
  #         [11, "test"],
  #         [12, "test2"],
  #       ])
  #     ensure
  #       t.exit
  #     end
  #   end
  # end

  context "idle message" do
    it "sends an idle message" do
      batch = Resque::Plugins::Batch.new
      batch.enqueue(LongJob)

      batch.message_handler.idle_handler = ->(_batch_jobs, msg) do
        expect(msg.keys).to eq([:duration])
        expect(msg[:duration]).to be > 0

        raise "SIGNAL"
      end

      expect {
        result = batch.perform
      }.to raise_error(StandardError, "SIGNAL")
    end
  end

  context "heartbeat" do
    before do
      stub_const("Resque::Plugins::Batch::JOB_HEARTBEAT", 1)
      stub_const("Resque::Plugins::Batch::JOB_HEARTBEAT_TTL", 2)
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

        batch = Resque::Plugins::Batch.new()
        batch.enqueue(LongJob)

        batch.message_handler.job_handler = ->(batch_jobs, job_id, job_msg) do
          msg = job_msg["msg"]

          if job_id == 0 && msg == 'begin'
            t.exit
          end
        end

        expect {
          batch.perform
        }.to raise_error(StandardError, "a job died...")
      ensure
        t.exit
      end
    end
  end

  context "a job with a failure" do
    it "return false" do
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

        batch = Resque::Plugins::Batch.new()
        batch.enqueue(Job, 11)
        batch.enqueue(Job, 12, "ERROR")
        batch.enqueue(Job, 13)

        result = batch.perform

        expect(result).to be_falsey

        expect(batch.batch_jobs[0].status).to eq("success")
        expect(batch.batch_jobs[0].klass).to eq(Job)
        expect(batch.batch_jobs[0].args).to eq([11])
        expect(batch.batch_jobs[0].result).to eq("test-success")
        expect(batch.batch_jobs[0].exception).to eq(nil)
        expect(batch.batch_jobs[0].duration).to be > 0

        expect(batch.batch_jobs[1].status).to eq("failure")
        expect(batch.batch_jobs[1].klass).to eq(Job)
        expect(batch.batch_jobs[1].args).to eq([12, "ERROR"])
        expect(batch.batch_jobs[1].result).to eq("ERROR")
        expect(batch.batch_jobs[1].exception).to eq(nil)
        expect(batch.batch_jobs[1].duration).to be > 0

        expect(batch.batch_jobs[2].status).to eq("success")
        expect(batch.batch_jobs[2].klass).to eq(Job)
        expect(batch.batch_jobs[2].args).to eq([13])
        expect(batch.batch_jobs[2].result).to eq("test-success")
        expect(batch.batch_jobs[2].exception).to eq(nil)
        expect(batch.batch_jobs[2].duration).to be > 0

      ensure
        t.exit
      end
    end
  end

  context "a job with an exception" do
    it "return false" do
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

        batch = Resque::Plugins::Batch.new()
        batch.enqueue(Job, 11)
        batch.enqueue(Job, 12, "EXCEPTION")
        batch.enqueue(Job, 13)

        result = batch.perform

        expect(result).to be_falsey

        expect(batch.batch_jobs[0].status).to eq("success")
        expect(batch.batch_jobs[0].klass).to eq(Job)
        expect(batch.batch_jobs[0].args).to eq([11])
        expect(batch.batch_jobs[0].result).to eq("test-success")
        expect(batch.batch_jobs[0].exception).to eq(nil)
        expect(batch.batch_jobs[0].duration).to be > 0

        expect(batch.batch_jobs[1].status).to eq("exception")
        expect(batch.batch_jobs[1].klass).to eq(Job)
        expect(batch.batch_jobs[1].args).to eq([12, "EXCEPTION"])
        expect(batch.batch_jobs[1].result).to eq(nil)
        expect(batch.batch_jobs[1].exception.keys).to eq(["class", "message", "backtrace"])
        expect(batch.batch_jobs[1].exception["class"]).to eq("RuntimeError")
        expect(batch.batch_jobs[1].exception["message"]).to eq("Unknown Exception")
        expect(batch.batch_jobs[1].duration).to be > 0

        expect(batch.batch_jobs[2].status).to eq("success")
        expect(batch.batch_jobs[2].klass).to eq(Job)
        expect(batch.batch_jobs[2].args).to eq([13])
        expect(batch.batch_jobs[2].result).to eq("test-success")
        expect(batch.batch_jobs[2].exception).to eq(nil)
        expect(batch.batch_jobs[2].duration).to be > 0
      ensure
        t.exit
      end
    end
  end
end
