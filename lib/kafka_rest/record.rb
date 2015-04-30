require 'base64'

module KafkaRest
  class Record
    attr_accessor :topic, :partition, :key, :value, :offset, :error, :error_code

    def initialize(topic: nil, partition: nil, key: nil, value: nil, offset: nil)
      @topic, @partition = topic, partition
      @key, @value, @offset = key, value, offset
    end

    def binary
      json = { value: Base64.strict_encode64(value) }
      json[:key]       = Base64.strict_encode64(key) unless key.nil?
      json[:partition] = partition unless partition.nil?
      return json
    end
  end
end
