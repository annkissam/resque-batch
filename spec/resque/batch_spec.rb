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

# NOTE: Normally this will be async, but it's easier to test this way
def process_job(queue)
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
