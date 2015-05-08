require 'test_helper'

class FunctionalBinaryConsumerTest < MiniTest::Test
  def setup
    @zk = ZookeeperHelper.new(ENV['ZOOKEEPER_URL'] || "localhost:2181")
    @url = ENV['KAFKA_REST_URL'] || "http://localhost:8082"
    @client = KafkaRest::Client.new(@url, ENV['KAFKA_REST_USERNAME'], ENV['KAFKA_REST_PASSWORD'])
  end

  def teardown
    @client.close
  end

  def test_low_level_consumer_calls
    @zk.delete_consumergroup('test.kafka_rest.low_level_binary')

    consumer = @client.consumer('test.kafka_rest.low_level_binary', offset_reset: 'largest')

    consumed = []
    t1 = Thread.new do
      while consumed.length < 4
        consumed += consumer.fetch_messages('test.1')
      end
    end

    sleep(1)

    produced = @client.topic('test.1').produce([
      KafkaRest::Record.new(value: "test 1"),
      KafkaRest::Record.new(value: "test 2"),
      KafkaRest::Record.new(value: "test 3"),
      KafkaRest::Record.new(value: "test 4"),
    ])

    t1.join


    assert_produced_equals_consumed produced, consumed

    committed = consumer.commit
    assert_equal 1, committed.length
    assert_equal 'test.1', committed[0][:topic]
    assert_equal 0, committed[0][:partition]
    assert_equal committed[0][:committed], committed[0][:consumed]
    assert_equal produced.last.offset, committed[0][:consumed]
  ensure
    consumer.close if consumer
  end

  def test_high_level_consumer
    @zk.delete_consumergroup('test.kafka_rest.high_level_binary')

    consumed_messages = []
    consumer_thread = Thread.new do
      consumer = @client.consumer('test.kafka_rest.high_level_binary', offset_reset: 'largest')
      consumer.consume('test.64') do |message|
        consumed_messages << message
        break if consumed_messages.length >= 100
      end
    end

    sleep(1)

    produced_messages = []
    producer_thread = Thread.new do
      100.times do |i|
        produced_messages += @client.topic('test.64').produce(KafkaRest::Record.new(value: "test #{i}"))
      end
    end

    producer_thread.join
    consumer_thread.join

    assert_produced_equals_consumed produced_messages, consumed_messages
  end

  def test_resume_consuming
    @zk.delete_consumergroup('test.kafka_rest.resume_consuming')

    consumed_messages = []
    consumer_thread_1 = Thread.new do
      consumer_1 = @client.consumer('test.kafka_rest.resume_consuming', offset_reset: 'largest')
      consumer_1.consume('test.64') do |message|
        consumed_messages << message
        break if consumed_messages.length >= 50
      end
    end

    sleep(1)

    produced_messages = []
    producer_thread_1 = Thread.new do
      50.times do |i|
        produced_messages += @client.topic('test.64').produce(KafkaRest::Record.new(value: "test #{i}"))
      end
    end

    producer_thread_1.join
    consumer_thread_1.join

    50.times do |i|
      produced_messages += @client.topic('test.64').produce(KafkaRest::Record.new(value: "test #{i + 50}"))
    end

    consumer_thread_2 = Thread.new do
      consumer_2 = @client.consumer('test.kafka_rest.resume_consuming')
      consumer_2.consume('test.64') do |message|
        consumed_messages << message
        break if consumed_messages.length >= 100
      end
    end

    consumer_thread_2.join
    assert_produced_equals_consumed produced_messages, consumed_messages
  end

  def test_multiple_concurrent_consumers
    @zk.delete_consumergroup('test.kafka_rest.multiple_concurrent_consumers')

    consumed_messages, mutex = [], Mutex.new

    consumer_thread_1 = Thread.new do
      client_1 = KafkaRest::Client.new(@url, ENV['KAFKA_REST_USERNAME'], ENV['KAFKA_REST_PASSWORD'])
      consumer_1 = client_1.consumer('test.kafka_rest.multiple_concurrent_consumers', offset_reset: 'largest')
      consumer_1.consume('test.4') do |message|
        Thread.current[:consumed] = true
        mutex.synchronize { consumed_messages << message }
      end
    end

    consumer_thread_2 = Thread.new do
      client_2 = KafkaRest::Client.new(@url, ENV['KAFKA_REST_USERNAME'], ENV['KAFKA_REST_PASSWORD'])
      consumer_2 = client_2.consumer('test.kafka_rest.multiple_concurrent_consumers', offset_reset: 'largest')
      consumer_2.consume('test.4') do |message|
        Thread.current[:consumed] = true
        mutex.synchronize { consumed_messages << message }
      end
    end

    consumer_thread_3 = Thread.new do
      client_3 = KafkaRest::Client.new(@url, ENV['KAFKA_REST_USERNAME'], ENV['KAFKA_REST_PASSWORD'])
      consumer_3 = client_3.consumer('test.kafka_rest.multiple_concurrent_consumers', offset_reset: 'largest')
      consumer_3.consume('test.4') do |message|
        Thread.current[:consumed] = true
        mutex.synchronize { consumed_messages << message }
      end
    end

    sleep(5)

    produced_messages = []
    Thread.new do
      300.times do |i|
        produced_messages += @client.topic('test.4').produce(KafkaRest::Record.new(value: "test #{i}"))
      end
    end.join

    while consumed_messages.length < 300
      sleep(0.1)
    end

    assert consumer_thread_1[:consumed]
    assert consumer_thread_2[:consumed]
    assert consumer_thread_3[:consumed]

    consumer_thread_1.kill
    consumer_thread_2.kill
    consumer_thread_3.kill

    assert_produced_equals_consumed produced_messages, consumed_messages
  end

  private

  def assert_produced_equals_consumed(produced_messages, consumed_messages)
    assert_equal produced_messages.length, consumed_messages.length, "Expected the total number of messages consumed to be equal to produced"

    consumed_messages_grouped = consumed_messages.group_by { |msg| msg.partition }
    produced_messages_grouped = produced_messages.group_by { |msg| msg.partition }

    assert_equal consumed_messages_grouped.keys.sort, produced_messages_grouped.keys.sort

    produced_messages_grouped.each do |partition, produced|
      consumed = consumed_messages_grouped[partition]
      produced.each.with_index do |message, index|
        assert_equal message.topic, consumed[index].topic
        assert_equal message.key, consumed[index].key
        assert_equal message.value, consumed[index].value
        assert_equal message.offset, consumed[index].offset
      end
    end
  end
end
