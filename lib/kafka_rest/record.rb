module KafkaRest
  class Record < Struct.new(:topic, :partition, :key, :value, :offset)
  end
end
