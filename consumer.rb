#!/usr/bin/env ruby

require_relative 'queue'

q = Queue.new

pp "consumer name: #{q.send(:redis_consumer_name)}"

loop do
  array = q.fetch
  #next if array.empty?

  pp array

  q.ack(array.map(&:first))

  sleep 2
end


=begin
redis_stream_key = 'bluehaze-events-stream'
redis_stream_group =  'bluehaze-queue-group'
redis_consumer_name = 'ololo consumer' #Process.pid.to_s
batch_size = 5

redis = Redis.new

begin
  redis.xgroup('CREATE', redis_stream_key, redis_stream_group, '$', mkstream: true)
rescue => e
  sap e
end

loop do
  array = redis.xreadgroup(redis_stream_group, redis_consumer_name, redis_stream_key, '>', block: 0, count: batch_size).fetch(redis_stream_key)
  ids = array.map(&:first)
  json_strings = array.map { |i| i[1]['json'] }
  sap array.map { |i| [i[0], i[1]['json']] }

  #id, data = redis.xreadgroup(redis_stream_group, redis_consumer_name, redis_stream_key, '>', block: 0, count: batch_size).fetch(redis_stream_key).first
  #ap data
  redis.multi do
    #redis.xack(redis_stream_key, redis_stream_group, *ids)
    #redis.xdel(redis_stream_key, *ids)
  end
  sleep 1
end
=end
