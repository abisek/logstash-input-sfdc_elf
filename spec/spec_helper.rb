# SimpleCov must be at the top of test class.
require 'simplecov'
SimpleCov.start

require 'logstash/devutils/rspec/spec_helper'
require 'lib/logstash/Inputs/sfdc_elf'

RSpec.configure do |config|
  # Use color in STDOUT
  config.color = true

  # Use color not only in STDOUT but also in pagers and files
  config.tty = true

  # Use the specified formatter
  config.formatter = :documentation # :progress, :html, :textmate
end