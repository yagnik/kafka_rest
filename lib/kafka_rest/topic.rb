module KafkaRest
  class Topic
    include KafkaRest::Producer

    attr_reader :client, :name

    def self.all(client:)
      client.request(:get, '/topics').collect do |topic|
        self.new(name: topic, client: client)
      end
    end

    def initialize(opts={})
      build(opts)
    end

    def configs
      sync unless @configs
      @configs
    end

    def partitions
      sync unless @partitions
      @partitions
    end

    private
    def build(opts={})
      @client = opts[:client]
      @name = opts[:name]
      @configs = opts[:configs]
      if opts.has_key?(:partitions)
        @partitions = opts[:partitions].collect do |partition|
          Partition.new(partition.merge(topic: @name))
        end
      end
    end

    def sync
      response = client.request(:get, "/topics/#{@name}")
      build(response)
    end
  end
end
