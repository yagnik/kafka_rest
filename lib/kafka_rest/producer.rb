module KafkaRest
  module Producer
    def produce(type:, event:)
      VALID_TYPES = [:avro, :binary]
      case type
      when :avro
        AvroProducer.produce
      when :binary
        BinaryProducer.produce
      else
        raise InvalidContentType
      end
    end
  end
end
