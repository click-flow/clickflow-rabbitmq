require 'bunny'
require 'json'

conn = Bunny.new('amqp://rabbitmq:cfrmq9988@localhost:5672')
conn.start
chan = conn.create_channel
queue = chan.queue('cftask', :durable => true, :auto_delete => false)

def runJob(msg)
  puts msg["thisis"]
end

begin
  queue.subscribe(:manual_ack => true, block: true) do |_delivery_info, _properties, body|
    runJob(JSON.parse(body))
    # this won't happen if the job fails
    chan.acknowledge(_delivery_info.delivery_tag, false)
  end
rescue Interrupt => _
  conn.close

  exit(0)
end

