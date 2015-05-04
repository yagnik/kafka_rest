require 'net/http'
require 'json'

module KafkaRest
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
      request(:get, '/topics').inject({}) do |result, topic|
        result[topic] = KafkaRest::Topic.new(self, topic)
        result
      end
    end

    def brokers
      request(:get, '/brokers')['brokers']
    end

    def request(method, path, body: nil, content_type: nil)
      Net::HTTP.start(endpoint.host, endpoint.port, use_ssl: endpoint.scheme == 'https'.freeze) do |http|
        request_class = case method
          when :get;    Net::HTTP::Get
          when :post;   Net::HTTP::Post
          when :put;    Net::HTTP::Put
          when :delete; Net::HTTP::Delete
          else raise ArgumentError, "Unsupported request method"
        end

        request = request_class.new(path)
        request.basic_auth(username, password) if username && password
        request['Accept'.freeze] = content_type.accept

        if body
          request['Content-Type'.freeze] = content_type.format
          request.body = JSON.dump(body)
        end

        case response = http.request(request)
        when Net::HTTPSuccess
          begin
            JSON.parse(response.body)
          rescue JSON::ParserError => e
            raise KafkaRest::ResponseError, "Invalid JSON in response: #{e.message}"
          end

        when Net::HTTPForbidden
          message = username.nil? ? "Unauthorized" : "User `#{username}` failed to authenticate"
          raise KafkaRest::UnauthorizedRequest.new(response.code.to_i, message)

        else
          response_data = begin
            JSON.parse(response.body)
          rescue JSON::ParserError => e
            raise KafkaRest::ResponseError, "Invalid JSON in response: #{e.message}"
          end

          error_class = RESPONSE_ERROR_CODES[response_data['error_code']] || KafkaRest::ResponseError
          raise error_class.new(response_data['error_code'], response_data['message'])
        end
      end
    end
  end
end
