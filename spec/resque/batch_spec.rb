require "spec_helper"

RSpec.describe Resque::Batch do
  it "has a version number" do
    expect(Resque::Batch::VERSION).not_to be nil
  end

  it "does something useful" do
    expect(false).to eq(true)
  end
end
