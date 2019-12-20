# coding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "resque/plugins/batch/version"

Gem::Specification.new do |spec|
  spec.name          = "resque-batch"
  spec.version       = Resque::Plugins::Batch::VERSION
  spec.authors       = ["Eric Sullivan"]
  spec.email         = ["eric.sullivan@annkissam.com"]

  spec.summary       = %q{Batch Job functionality for Resque}
  spec.description   = %q{Adds methods for working with Batch Jobs}
  spec.homepage      = "https://github.com/annkissam/resque-batch"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  # if spec.respond_to?(:metadata)
  #   spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"
  # else
  #   raise "RubyGems 2.0 or newer is required to protect against " \
  #     "public gem pushes."
  # end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "resque", "~> 1.25"

  spec.add_development_dependency "bundler", "~> 2.0"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "bundler-audit"
  spec.add_development_dependency "rubocop", '~> 0.78'
  spec.add_development_dependency "rspec_junit_formatter"
end
