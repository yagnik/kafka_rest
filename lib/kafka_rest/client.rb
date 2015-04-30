require 'net/http'
require 'json'

module KafkaRest
  class ResponseError < Error
    attr_reader :code

    def initialize(code, message)
      @code = code
      super("#{message} (error code #{code})")
    end
  end

  InvalidResponse = Class.new(KafkaRest::Error)

  RESPONSE_ERROR_CODES = {
    40401 => (SubjectNotFound           = Class.new(KafkaRest::ResponseError)),
    40402 => (VersionNotFound           = Class.new(KafkaRest::ResponseError)),
    40403 => (SchemaNotFound            = Class.new(KafkaRest::ResponseError)),
    42201 => (InvalidAvroSchema         = Class.new(KafkaRest::ResponseError)),
    42202 => (InvalidVersion            = Class.new(KafkaRest::ResponseError)),
    42203 => (InvalidCompatibilityLevel = Class.new(KafkaRest::ResponseError)),
    409   => (IncompatibleAvroSchema    = Class.new(KafkaRest::ResponseError)),
    403   => (UnauthorizedRequest       = Class.new(KafkaRest::ResponseError)),
  }

  class Client
    attr_reader :endpoint, :username, :password

    def initialize(endpoint, username = nil, password = nil)
      @endpoint = URI(endpoint)
      @username, @password = username, password
    end

    def topic(name)
      KafkaRest::Topic.new(self, name)
    end

    def topics
      result = {}
      request(:get, '/topics').each do |topic|
        result[topic] = KafkaRest::Topic.new(self, topic)
      end
      result
    end

    def request(method, path, body: nil, content_type: nil)
      Net::HTTP.start(endpoint.host, endpoint.port, use_ssl: endpoint.scheme == 'https') do |http|
        request_class = case method
          when :get;    Net::HTTP::Get
          when :post;   Net::HTTP::Post
          when :put;    Net::HTTP::Put
          when :delete; Net::HTTP::Delete
          else raise ArgumentError, "Unsupported request method"
        end

        request = request_class.new(path)
        request.basic_auth(username, password) if username && password
        request['Accept'] = "application/vnd.kafka.v1+json; q=0.9, application/json; q=0.5"

        if body
          request['Content-Type'] = content_type || "application/json"
          request.body = JSON.dump(body)
        end

        case response = http.request(request)
        when Net::HTTPSuccess
          begin
            JSON.parse(response.body)
          rescue JSON::ParserError => e
            raise KafkaRest::InvalidResponse, "Invalid JSON in response: #{e.message}"
          end

        when Net::HTTPForbidden
          message = username.nil? ? "Unauthorized" : "User `#{username}` failed to authenticate"
          raise KafkaRest::UnauthorizedRequest.new(response.code.to_i, message)

        else
          response = begin
            JSON.parse(response.body)
          rescue JSON::ParserError => e
            raise KafkaRest::InvalidResponse, "Invalid JSON in response: #{e.message}"
          end

          error_class = RESPONSE_ERROR_CODES[response_data['error_code']] || KafkaRest::ResponseError
          raise error_class.new(response_data['error_code'], response_data['message'])
        end
      end
    end
  end
end
