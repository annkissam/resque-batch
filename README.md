# Resque - Batch

A plugin for Resque that allow for batched jobs. It provides two components: a batch and a wrapper around a normal Resque job. The wrapper handles communicating to the batch through a redis list, providing status updates as the jobs are processed. This allow the main program to call 'perform' on a batch, and wait for results that are processed by multiple resque workers.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'resque-batch', git: 'git@github.com:annkissam/resque-batch.git'
```

And then execute:

    $ bundle


## Usage

### Create a Batch Worker

* include 'Resque::Plugins::Batch::Job'
* the method is perform_work (not perform)
* You should return `success, message`

```ruby
class Archive
  include Resque::Plugins::Batch::Job

  def self.perform_work(repo_id, branch = 'master')
    repo = Repository.find(repo_id)
    repo.create_archive(branch)

    return true, nil
  end
end
```

### Create a Batch (and call perform)

```ruby
batch = Resque::Plugins::Batch.new
batch.enqueue(Job, 11)
batch.enqueue(Job, 12, "test2")
result = batch.perform
```

## message_handler

You can process results as they arrive w/ a message_handler:

```ruby
batch.init_handler do |batch_jobs|
  puts "Notify client it's starting"
end

batch.exit_handler do |batch_jobs|
  puts "You're done!"
end

batch.job_begin_handler do |batch_jobs, job_id|
  puts "Job #{job_id} started w/ params #{batch_jobs[job_id].args}"
end

batch.job_success_handler do |batch_jobs, job_id, data|
  puts "Job #{job_id} succeeded w/ results #{data}"
end

result = batch.perform
```

If you need to send additional notifications there's an 'info' message

```ruby
class Archive
  include Resque::Plugins::Batch::Job
  
  def self.perform_work(repo_id, branch = 'master')
    ...
      @worker_job_info.info!({your: "DATA"})
    ...
  end
end

batch.job_info_handler do |batch_jobs, job_id, data|
  puts "Job #{job_id} sent info with your: #{data["your"]}"
end
```


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

### Testing

This uses Resque which requires Redis. To test:

```
$brew install redis
$redis-server /usr/local/etc/redis.conf
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/annkissam/resque-batch.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
