module KafkaRest
  class Topic
    include KafkaRest::Producer

    attr_reader :client, :name

    def initialize(client, name, partitions: nil, configs: nil)
      @client, @name = client, name
      @partitions, @configs = partitions, configs
    end

    def configs
      sync unless @configs
      @configs
    end

    def partitions
      sync unless @partitions
      @partitions
    end

    def topic
      self
    end

    def partition(id)
      KafkaRest::Partition.new(client, self, id)
    end

    def path
      "/topics/#{name}"
    end

    def ==(other)
      return false unless other.is_a?(KafkaRest::Topic)
      client == other.client && other.name == name
    end

    alias_method :eql?, :==

    def hash
      [client, topic.name].hash
    end

    private

    def sync
      response = client.request(:get, path)
      @configs    = response.fetch(:configs)
      @partitions = response.fetch(:partitions).map do |partition|
        KafkaRest::Partition.from_json(client, self, partition)
      end
    end
  end
end
