module KafkaRest
  class Consumer

    attr_reader :client, :group, :format, :instance_id, :base_uri

    def initialize(temporary_client, group, options = {})
      @group = group
      @messages_fetched = false
      register(temporary_client, options)
    end

    def register(temporary_client, instance_id: nil, format: :binary, offset_reset: nil, auto_commit: nil)
      payload = {}
      payload['format']             = format ||:binary
      payload['id']                 = instance_id  unless instance_id.nil?
      payload['auto.offset.reset']  = offset_reset unless offset_reset.nil?
      payload['auto.commit.enable'] = auto_commit  unless auto_commit.nil?

      response = temporary_client.request(:post, "/consumers/#{group}", body: payload)

      @format      = format || :binary
      @instance_id = response.fetch(:instance_id)
      @base_uri    = response.fetch(:base_uri)
      @client      = KafkaRest::Client.new(@base_uri)
    end

    def consume(topic, &block)
      messages_fetched, exception_occurred = false, false
      loop do
        messages = fetch_messages(topic)
        messages_fetched ||= messages.length > 0
        messages.each(&block)
      end
    rescue Exception
      exception_occurred = true
      raise
    ensure
      commit if messages_fetched && !exception_occurred
      close
    end

    def fetch_messages(topic)
      response = client.request(:get, "/consumers/#{group}/instances/#{instance_id}/topics/#{topic}", accept: KafkaRest::Client::BINARY_CONTENT_TYPE)
      response.map do |message|
        message[:topic] = topic
        message[:key]   = decode_embedded_format(message[:key])   unless message[:key].nil?
        message[:value] = decode_embedded_format(message[:value]) unless message[:value].nil?
        KafkaRest::Record.new(message)
      end
    end

    def commit
      client.request(:post, "/consumers/#{group}/instances/#{instance_id}/offsets")
    end

    def deregister
      client.request(:delete, "/consumers/#{group}/instances/#{instance_id}")
    end

    def close
      deregister
      client.close
      @client = nil
    end

    protected

    def decode_embedded_format(value)
      case format
        when :binary; Base64.strict_decode64(value)
        else raise ArgumentError, "Unsupported format: #{format}"
      end
    end
  end
end
