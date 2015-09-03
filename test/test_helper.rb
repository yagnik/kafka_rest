require 'minitest/autorun'
require 'minitest/unit'
require 'mocha/mini_test'
require 'kafka_rest'

require 'pry'

require 'zookeeper'


class ZookeeperHelper
  attr_reader :zookeeper

  def initialize(zookeeper_connection_string)
    @zookeeper = Zookeeper.new(zookeeper_connection_string)
  end

  def delete_consumergroup(name)
    recursive_delete("/consumers/#{name}")
  end

  protected

  def recursive_delete(path)
    Array(zookeeper.get_children(:path => path)[:children]).each do |child|
      recursive_delete(File.join(path, child))
    end
    zookeeper.delete(:path => path)
  end
end
