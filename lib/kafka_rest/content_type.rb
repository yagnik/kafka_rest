module KafkaRest
  class ContentType
    VALID_EMBEDDED_FORMATS = %i(binary avro).freeze
    VALID_API_VERSIONS = [1].freeze
    VALID_SERIALIZATION_FORMATS = %i(json).freeze

    attr_reader :embedded_format, :api_version, :serialization_format

    def initialize(embedded_format=:binary, api_version=1, serialization_format=:json)
      validate!(embedded_format, VALID_EMBEDDED_FORMATS)
      validate!(api_version, VALID_API_VERSIONS)
      validate!(serialization_format, VALID_SERIALIZATION_FORMATS)

      @embedded_format, @api_version, @serialization_format = embedded_format, api_version, serialization_format
    end

    def format
      "application/vnd.kafka.#{embedded_format}.#{api_version}+#{serialization_format}"
    end

    private
    def validate!(value, list)
      raise KafkaRest::InvalidContentType unless list.include?(value)
      return true
    end
  end
end
