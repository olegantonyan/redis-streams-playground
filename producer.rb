#!/usr/bin/env ruby

require_relative 'queue'

q = Queue.new

loop do
  q.push(Time.now.to_s)

  sleep 1
end
