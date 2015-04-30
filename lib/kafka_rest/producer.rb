module KafkaRest
  module Producer
    def produce(type, event)
      case type
      when :avro
        AvroProducer.produce
      when :binary
        BinaryProducer.produce
      else
        raise InvalidContentType
      end
    end

    VALID_TYPES = [:avro, :binary].freeze
    private_constant :VALID_TYPES
  end
end
