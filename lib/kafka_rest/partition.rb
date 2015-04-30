module KafkaRest
  class Topic
    class Partition
      include KafkaRest::Producer

      Replica = Struct.new(:broker, :leader, :in_sync)

      attr_reader :topic_name, :partition, :leader, :replicas

      def initialize(opts={})
        @topic_name = opts[:topic]
        @partition = opts[:partition]
        @leader = opts[:leader]
        @replicas = opts[:replicas].map do |replica|
          Replica.new(*replica.slice(Replica.members).values)
        end
      end
    end
  end
end
