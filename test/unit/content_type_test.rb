require 'test_helper'

class ContentTypeTest < Minitest::Test
  def test_default_format_is_set
    content_type = KafkaRest::ContentType.new
    assert_equal :binary, content_type.embedded_format
    assert_equal 1,       content_type.api_version
    assert_equal :json,   content_type.serialization_format
  end

  def test_raises_if_format_is_invalid
    assert_raises KafkaRest::InvalidContentType do
      KafkaRest::ContentType.new(:random)
    end

    assert_raises KafkaRest::InvalidContentType do
      KafkaRest::ContentType.new(:binary, 2)
    end

    assert_raises KafkaRest::InvalidContentType do
      KafkaRest::ContentType.new(:binary, 1, :random)
    end
  end
end
