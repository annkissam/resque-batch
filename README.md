# Resque::Batch

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

* include 'Resque::Batch::Job'
* the method is perform_job (not perform)
* You should return `success, message`

```
class Archive
  include Resque::Batch::Job

  def self.perform_job(repo_id, branch = 'master')
    repo = Repository.find(repo_id)
    repo.create_archive(branch)

    return true, nil
  end
end
```

### Create a Batch

You can wait for the result:

```
batch = Resque::Batch.new()
batch.enqueue(Job, 11)
batch.enqueue(Job, 12, "test2")
result = batch.perform
```

Or you can process results as they arrive:

```
result = batch.perform do |batch_jobs, msg, data|
  case msg
  when :init
    "Notify client it's starting"
  when :status
    "Keep track of state"
  when :exit
    "You're done!"
  else
    raise "Unknown message #{msg}"
  end
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
