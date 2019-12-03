require 'bunny'
conn = Bunny.new('amqp://rabbitmq:cfrmq9988@localhost:5672')
conn.start
chan = conn.create_channel
queue = chan.queue('cftask', :durable => true, :auto_delete => false)


queue.publish('{ "thisis": "a test from ruby" }', persistent: true, routing_key: queue.name)

