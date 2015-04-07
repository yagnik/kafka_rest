module KafkaRest
  class Broker < KafkaRest::Client
    self << class
      def all
        get(path: '/brokers').collect do |broker_id|
          self.new(id: broker_id)
        end
      end
    end

    attr_reader :id

    def initialize(id:)
      @id = id
    end
  end
end
