require 'test_helper'

class FunctionalBinaryProducerTest < MiniTest::Test
  def setup
    url = ENV['KAFKA_REST_URL'] || "http://localhost:8082"
    @client = KafkaRest::Client.new(url, ENV['KAFKA_REST_USERNAME'], ENV['KAFKA_REST_PASSWORD'])
  end

  def test_producing_to_topic
    records = @client.topic("test.1").produce(KafkaRest::Record.new(value: 'testing 133'))
    assert_equal 1, records.length
    assert_equal 'testing 133', records[0].value
    assert_equal 0, records[0].partition
    assert_equal 'test.1', records[0].topic.name
  end

  def test_producing_to_partition
    records = @client.topic("test.4").partition(3).produce(KafkaRest::Record.new(value: 'testing 133'))
    assert_equal 1, records.length
    assert_equal 'testing 133', records[0].value
    assert_equal 3, records[0].partition
    assert_equal 'test.4', records[0].topic.name
    assert records[0].offset >= 0
  end

  def test_produce_batch_with_random_partitioning
    records = (0...1000).map { |index| KafkaRest::Record.new(value: "testing #{index}") }
    produced_records = @client.topic("test.4").produce(records)

    assert_equal 1000, produced_records.length
    groups = produced_records.group_by(&:partition)
    assert_equal 4, groups.length
    assert produced_records.all? { |r| r.offset >= 0 }
    assert produced_records.all? { |r| r.error_code.nil? }
  end
end
