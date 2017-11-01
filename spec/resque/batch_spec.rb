require "spec_helper"

class Job
  include Resque::Batch::Job

  def self.perform_work(id, data = "test")
    @processed_jobs << [id, data]

    return true, data + "-success"
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
  include Resque::Batch::Job

  def self.perform_work
    sleep(60)

    return true, nil
  end
end

# NOTE: Normally this will be async, but it's easier to test this way
def process_job(queue = :batch)
  worker = Resque::Worker.new(:batch)
  worker.fork_per_job = false
  worker.work_one_job
end

RSpec.describe Resque::Batch do
  before do
    Job.reset_jobs

    Resque::Stat.clear(:processed)
    Resque::Stat.clear(:failed)

    Resque.remove_queue(:batch)
  end

  it "has a version number" do
    expect(Resque::Batch::VERSION).not_to be nil
  end

  context "Resque.inline == false" do
    before do
      Resque.inline = false
    end

    it "works (with a block)" do
      batch = Resque::Batch.new()
      batch.enqueue(Job, 11)
      batch.enqueue(Job, 12, "test2")

      expect(Resque.size(:batch)).to eq(0)
      expect(Resque::Stat[:processed]).to eq(0)
      expect(Resque::Stat[:failed]).to eq(0)
      expect(Job.processed_jobs).to eq([
      ])

      result = batch.perform do |batch_jobs, msg, data|
        case msg
        when :init
          expect(batch_jobs[0].status).to eq("pending")
          expect(batch_jobs[0].batch_id).to eq(batch.id)
          expect(batch_jobs[0].job_id).to eq(0)

          expect(batch_jobs[1].status).to eq("pending")
          expect(batch_jobs[1].batch_id).to eq(batch.id)
          expect(batch_jobs[1].job_id).to eq(1)

          # Kickoff an initial Job
          process_job(:batch)
        when :status
          id = data["id"]
          status = data["status"]

          if id == 0 && status == 'running'
            expect(batch_jobs[0].status).to eq("running")
            expect(batch_jobs[0].klass).to eq(Job)
            expect(batch_jobs[0].args).to eq([11])
            expect(batch_jobs[0].msg).to eq(nil)
            expect(batch_jobs[0].exception).to eq(nil)

          elsif id == 0 && status == 'success'
            expect(batch_jobs[0].status).to eq("success")
            expect(batch_jobs[0].klass).to eq(Job)
            expect(batch_jobs[0].args).to eq([11])
            expect(batch_jobs[0].msg).to eq("test-success")
            expect(batch_jobs[0].exception).to eq(nil)

            # Kickoff a second Job
            process_job(:batch)

          elsif id == 1 && status == 'running'
            expect(batch_jobs[1].status).to eq("running")
            expect(batch_jobs[1].klass).to eq(Job)
            expect(batch_jobs[1].args).to eq([12, "test2"])
            expect(batch_jobs[1].msg).to eq(nil)
            expect(batch_jobs[1].exception).to eq(nil)

          elsif id == 1 && status == 'success'
            expect(batch_jobs[1].status).to eq("success")
            expect(batch_jobs[1].klass).to eq(Job)
            expect(batch_jobs[1].args).to eq([12, "test2"])
            expect(batch_jobs[1].msg).to eq("test2-success")
            expect(batch_jobs[1].exception).to eq(nil)

          else
            raise "SPEC FAILURE"
          end
        when :exit
          expect(batch_jobs[0].status).to eq("success")
          expect(batch_jobs[1].status).to eq("success")
        else
          raise "Unknown message #{msg}"
        end
      end

      expect(result).to be_truthy

      expect(Resque.size(:batch)).to eq(0)
      expect(Resque::Stat[:processed]).to eq(2)
      expect(Resque::Stat[:failed]).to eq(0)
      expect(Job.processed_jobs).to eq([
        [11, "test"],
        [12, "test2"],
      ])
    end

    it "works (without a block)" do
      batch = Resque::Batch.new()
      batch.enqueue(Job, 11)
      batch.enqueue(Job, 12, "test2")

      expect(Resque.size(:batch)).to eq(0)
      expect(Resque::Stat[:processed]).to eq(0)
      expect(Resque::Stat[:failed]).to eq(0)
      expect(Job.processed_jobs).to eq([
      ])

      t = Thread.new {
        loop do
          if Resque.size(:batch) > 0
            process_job(:batch)
          else
            sleep(0.1)
          end
        end
      }

      result = batch.perform

      expect(result).to be_truthy

      expect(Resque.size(:batch)).to eq(0)
      expect(Resque::Stat[:processed]).to eq(2)
      expect(Resque::Stat[:failed]).to eq(0)
      expect(Job.processed_jobs).to eq([
        [11, "test"],
        [12, "test2"],
      ])

      t.exit
    end
  end

  # NOTE: Resque does not set Resque::Stat for inline
  context "Resque.inline == true" do
    before do
      Resque.inline = true
    end

    after do
      Resque.inline = false
    end

    it "works (with a block)" do
      batch = Resque::Batch.new()
      batch.enqueue(Job, 11)
      batch.enqueue(Job, 12, "test2")

      expect(Resque.size(:batch)).to eq(0)
      expect(Job.processed_jobs).to eq([
      ])

      result = batch.perform do |batch_jobs, msg, data|
        case msg
        when :init
          expect(batch_jobs[0].status).to eq("pending")
          expect(batch_jobs[0].batch_id).to eq(batch.id)
          expect(batch_jobs[0].job_id).to eq(0)

          expect(batch_jobs[1].status).to eq("pending")
          expect(batch_jobs[1].batch_id).to eq(batch.id)
          expect(batch_jobs[1].job_id).to eq(1)

          # Kickoff an initial Job
          process_job(:batch)
        when :status
          id = data["id"]
          status = data["status"]

          if id == 0 && status == 'running'
            expect(batch_jobs[0].status).to eq("running")
            expect(batch_jobs[0].klass).to eq(Job)
            expect(batch_jobs[0].args).to eq([11])
            expect(batch_jobs[0].msg).to eq(nil)
            expect(batch_jobs[0].exception).to eq(nil)

          elsif id == 0 && status == 'success'
            expect(batch_jobs[0].status).to eq("success")
            expect(batch_jobs[0].klass).to eq(Job)
            expect(batch_jobs[0].args).to eq([11])
            expect(batch_jobs[0].msg).to eq("test-success")
            expect(batch_jobs[0].exception).to eq(nil)

            # Kickoff a second Job
            process_job(:batch)

          elsif id == 1 && status == 'running'
            expect(batch_jobs[1].status).to eq("running")
            expect(batch_jobs[1].klass).to eq(Job)
            expect(batch_jobs[1].args).to eq([12, "test2"])
            expect(batch_jobs[1].msg).to eq(nil)
            expect(batch_jobs[1].exception).to eq(nil)

          elsif id == 1 && status == 'success'
            expect(batch_jobs[1].status).to eq("success")
            expect(batch_jobs[1].klass).to eq(Job)
            expect(batch_jobs[1].args).to eq([12, "test2"])
            expect(batch_jobs[1].msg).to eq("test2-success")
            expect(batch_jobs[1].exception).to eq(nil)

          else
            raise "SPEC FAILURE"
          end
        when :exit
          expect(batch_jobs[0].status).to eq("success")
          expect(batch_jobs[1].status).to eq("success")
        else
          raise "Unknown message #{msg}"
        end
      end

      expect(result).to be_truthy

      expect(Resque.size(:batch)).to eq(0)
      expect(Job.processed_jobs).to eq([
        [11, "test"],
        [12, "test2"],
      ])
    end

    it "works (without a block)" do
      batch = Resque::Batch.new()
      batch.enqueue(Job, 11)
      batch.enqueue(Job, 12, "test2")

      expect(Resque.size(:batch)).to eq(0)
      expect(Job.processed_jobs).to eq([
      ])

      t = Thread.new {
        loop do
          if Resque.size(:batch) > 0
            process_job(:batch)
          else
            sleep(0.1)
          end
        end
      }

      result = batch.perform

      expect(result).to be_truthy

      expect(Resque.size(:batch)).to eq(0)
      expect(Job.processed_jobs).to eq([
        [11, "test"],
        [12, "test2"],
      ])

      t.exit
    end
  end

  context "idle_callback_timeout" do
    it "send an idle message (with a block)" do
      batch = Resque::Batch.new()
      batch.enqueue(LongJob)

      expect {
        result = batch.perform(0.1) do |batch_jobs, msg, data|
          case msg
          when :idle
            raise "SIGNAL"
          end
        end
      }.to raise_error(StandardError, "SIGNAL")
    end

    it "raise an idle exception (without a block)" do
      batch = Resque::Batch.new()
      batch.enqueue(LongJob)

      expect {
        batch.perform(0.1)
      }.to raise_error(StandardError, "IDLE: there appears to be no activity")
    end
  end

  context "heartbeat" do
    before do
      stub_const("Resque::Batch::JOB_HEARTBEAT", 1)
      stub_const("Resque::Batch::JOB_HEARTBEAT_TTL", 2)
    end

    it "raises 'a job died' exception" do
      begin
        t = Thread.new do
          loop do
            process_job
          end
        end

        batch = Resque::Batch.new()
        batch.enqueue(LongJob)

        expect {
          batch.perform do |batch_jobs, msg, data|
            case msg
            when :status
              id = data["id"]
              status = data["status"]

              if id == 0 && status == 'running'
                t.exit
              end
            end
          end
        }.to raise_error(StandardError, "a job died...")
      ensure
        t.exit
      end
    end
  end


end
