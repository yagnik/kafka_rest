# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kafka_rest/version'

Gem::Specification.new do |spec|
  spec.name          = "kafka_rest"
  spec.version       = KafkaRest::VERSION
  spec.authors       = ["Yagnik"]
  spec.email         = ["yagnik.khanna@shopify.com"]
  spec.summary       = %q{Ruby gem to access kafka-rest by Confluent Inc}
  spec.description   = %q{Ruby gem to access kafka-rest by Confluent Inc}
  spec.homepage      = "http://confluent.io/docs/current/kafka-rest/docs/index.html"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "vcr", "~> 2.9.3"
  spec.add_development_dependency "mocha", "~> 1.0"
end
