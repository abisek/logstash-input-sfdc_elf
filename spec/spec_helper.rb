# SimpleCov must be at the top of test class.
require 'simplecov'
SimpleCov.start

require 'logstash/devutils/rspec/spec_helper'
require 'lib/logstash/inputs/sfdc_elf'
require 'webmock/rspec'
require 'timecop'



# Set up color and formatting for Rspec tests.
RSpec.configure do |config|
  # Use color in STDOUT
  config.color = true

  # Use color not only in STDOUT but also in pagers and files
  config.tty = true

  # Use the specified formatter
  config.formatter = :documentation # :progress, :html, :textmate
end



# Turn off all connection to the internet.
WebMock.disable_net_connect!(allow_localhost: true)



# Helper methods to make it simple getting fixture data.
def fixture_path
  File.expand_path('../fixtures', __FILE__)
end

def fixture(file)
  File.new(fixture_path + '/' + file)
end
