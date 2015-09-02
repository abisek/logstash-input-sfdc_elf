@files = []

require 'rubocop/rake_task'
RuboCop::RakeTask.new

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec)

require 'logstash/devutils/rake'

task test: :spec

task default: :test
