require 'kafka_rest/version'

module KafkaRest
  Error               = Class.new(StandardError)
  InvalidContentType  = Class.new(Error)
end

require 'kafka_rest/client'
require 'kafka_rest/producer'
require 'kafka_rest/topic'
require 'kafka_rest/partition'
require 'kafka_rest/record'
