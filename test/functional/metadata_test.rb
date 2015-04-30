require 'test_helper'

class FunctionalMetadataTest < Minitest::Test
  def setup
    url = ENV['KAFKA_REST_URL'] || "http://localhost:8082"
    @client = KafkaRest::Client.new(url, ENV['KAFKA_REST_USERNAME'], ENV['KAFKA_REST_PASSWORD'])
  end

  def test_topic_metadata
    topic = @client.topic("test.1")
    assert_equal "test.1", topic.name
    assert_equal 1, topic.partitions.length

    assert_equal topic, @client.topics['test.1']
  end

  def test_partition_metadata
    topic = @client.topic("test.1")
    partition = topic.partition(0)
    assert partition.leader >= 0
    assert partition.replicas.length > 0
    leading_replica = partition.replicas.detect { |r| r.leader }
    assert_equal partition.leader, leading_replica.broker

    assert_equal partition, topic.partitions[0]
  end
end
