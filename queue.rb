require 'redis'
require 'connection_pool'
require 'socket'

class Queue
  def initialize
    @batch_size = 5
    @maxlen = 10
    @pool = ConnectionPool.new(size: 3, timeout: 20) { Redis.new }

    create_group unless group_exists?

    self.check_backlog = true
    self.last_id = '0-0'
  end

  def push(string)
    pool.with do |redis|
      redis.xadd(redis_stream_key, { json: string }, maxlen: maxlen)
    end
  end

  def fetch
    reclaim if check_backlog
    pool.with do |redis|
      id = check_backlog ? last_id : '>'
      array = redis.xreadgroup(redis_stream_group, redis_consumer_name, redis_stream_key, id, count: batch_size).fetch(redis_stream_key, [])
      array.map! { |i| [i[0], i[1]['json']] } # [ id, json_string ]
      self.check_backlog = false if array.empty?
      self.last_id = array.map(&:first).max # TODO proper max
      array
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
    self.last_id = '0-0'
  end

  def len
    pool.with do |redis|
      redis.xlen(redis_stream_key)
    end
  end

  private

  attr_reader :pool, :batch_size, :maxlen
  attr_accessor :check_backlog, :last_id

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
      redis.exists?(redis_stream_key) && redis.xinfo(:groups, redis_stream_key).any? { |i| i['name'] == redis_stream_group }
    end
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
        pp "reclaim ids #{ids}"
        redis.xclaim(redis_stream_key, redis_stream_group, redis_consumer_name, 0, ids, justid: true)
      end
    end
  end
end
