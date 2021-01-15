#!/usr/bin/env ruby

require_relative 'queue'

q = Queue.new

loop do
  q.push((25 + rand * 5 - rand * 5).to_s)

  sleep 0.1
end
