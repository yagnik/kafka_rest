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
      body = { records: records.map(&:binary) }
      response = client.request(:post, path, body: body, content_type: KafkaRest::Client::BINARY_CONTENT_TYPE)

      response['offsets'].each_with_index do |offset, index|
        record = records[index]

        record.topic = topic
        if offset['error'].nil?
          record.offset    = offset['offset']
          record.partition = topic.partition(offset['partition'])
        else
          record.error      = offset['error']
          record.error_code = offset['error_code']
        end
      end

      records
    end

    def produce_avro(records, value_schema, key_schema)
      raise NotImplementedError
    end
  end
end
