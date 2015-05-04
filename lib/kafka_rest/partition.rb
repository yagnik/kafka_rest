module KafkaRest
  class Partition
    include KafkaRest::Producer

    Replica = Struct.new(:broker, :leader, :in_sync)

    attr_reader :client, :topic, :id

    def initialize(client, topic, id)
      @client, @topic, @id = client, topic, id
    end

    def leader
      sync unless @leader
      @leader
    end

    def replicas
      sync unless @replicas
      @replicas
    end

    def path
      "/topics/#{topic.name}/partitions/#{id}"
    end

    def sync
      response = client.request(:get, path)
      populate_from_json(response)
    end

    def populate_from_json(json_object)
      @leader   = json_object.fetch(:leader)
      @replicas = json_object.fetch(:replicas).map { |r| Replica.new(r.fetch(:broker), r.fetch(:leader), r.fetch(:in_sync)) }
      self
    end

    def self.from_json(client, topic, json_object)
      new(client, topic, json_object.fetch(:partition)).populate_from_json(json_object)
    end

    def ==(other)
      return false unless other.is_a?(KafkaRest::Partition)
      topic == other.topic && other.id == id
    end

    alias_method :eql?, :==

    def hash
      [topic, id].hash
    end
  end
end
