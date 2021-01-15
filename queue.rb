require 'redis'
require 'connection_pool'
require 'socket'

class Queue
  def initialize
    @batch_size = 5
    @maxlen = 1_000_000
    @pool = ConnectionPool.new(size: 3, timeout: 20) { Redis.new }

    create_group unless group_exists?
    reclaim
  end

  def push(string)
    pool.with do |redis|
      redis.xadd(redis_stream_key, { json: string }, maxlen: maxlen)
    end
  end

  def fetch
    return [] if len.zero?
    pool.with do |redis|
      array = redis.xreadgroup(redis_stream_group, redis_consumer_name, redis_stream_key, '>', count: batch_size).fetch(redis_stream_key)
      array.map { |i| [i[0], i[1]['json']] } # [ id, json_string ]
    end
  end

  def ack(ids)
    return if ids.empty?
    pool.with do |redis|
      redis.multi do
        redis.xack(redis_stream_key, redis_stream_group, *ids)
        redis.xdel(redis_stream_key, *ids)
      end
    end
  end

  def clear
    pool.with do |redis|
      redis.del(redis_stream_key)
    end
  end

  def len
    pool.with do |redis|
      redis.xlen(redis_stream_key)
    end
  end

  private

  attr_reader :pool, :batch_size, :maxlen

  def redis_stream_key
    'bluehaze-events-stream'
  end

  def redis_stream_group
    'bluehaze-queue-group'
  end

  def redis_consumer_name
    @redis_consumer_name ||= "bluehaze-consumer-#{Socket.gethostname}-#{Process.pid}"
  end

  def group_exists?
    pool.with do |redis|
      redis.xinfo(:groups, redis_stream_key).any? { |i| i['name'] == redis_stream_group }
    end
  rescue Redis::CommandError
    false
  end

  def create_group
    pool.with do |redis|
      redis.xgroup('CREATE', redis_stream_key, redis_stream_group, '$', mkstream: true)
    end
  end

  def reclaim
    pool.with do |redis|
      pending = redis.xpending(redis_stream_key, redis_stream_group, '-', '+', maxlen)
      ids = pending.map { |i| i['entry_id'] }
      if ids.any?
        redis.multi do
          pp "reclaim ids #{ids}"
          redis.xclaim(redis_stream_key, redis_stream_group, redis_consumer_name, 0, ids, justid: true)
        end
      end
    end
  end
end
