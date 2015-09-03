module KafkaRest
  module Producer
    def produce(records, value_schema: nil, key_schema: nil)
      if value_schema.nil?
        produce_binary(Array(records))
      else
        produce_avro(Array(records), value_schema, key_schema)
      end
    end

    def produce_binary(records)
      body = { records: records.map(&:as_json_with_embedded_binary) }
      response = client.request(:post, path, body: body, content_type: KafkaRest::Client::BINARY_CONTENT_TYPE)

      response.fetch(:offsets).each_with_index do |offset, index|
        record = records[index]

        record.topic = topic.name
        if offset.key?(:error)
          record.offset    = offset.fetch(:offset)
          record.partition = offset.fetch(:partition)
        else
          record.error      = offset.fetch(:error)
          record.error_code = offset.fetch(:error_code)
        end
      end

      records
    end

    def produce_avro(records, value_schema, key_schema)
      raise NotImplementedError
    end
  end
end
