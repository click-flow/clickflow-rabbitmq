const amqp = require('amqplib'),
   amqpUrl = 'amqp://rabbitmq:cfrmq9988@cog-l';


const CFPubSubJobs = {
  enqueueTask: async (q, data) => {
    const conn = await amqp.connect(amqpUrl);
    const chan = await conn.createChannel();
    return chan.assertQueue(q, { durable: true })
      .then(() => chan.sendToQueue(q, Buffer.from(data), { persistent: true }))
      .then(() => chan.close());
  },
  getAndRunTask: async (chan, q, job) => {
    const acknowledgeReceipt = async msg => {
      const jobRes = await job(msg);
      return chan.ack(msg);
    };
    return chan.assertQueue(q, { durable: true })
      .then(() => chan.prefetch(1))
      .then(() => chan.consume(q, acknowledgeReceipt, { noAck: false }));
  },
  listen: async (q, job) => {
    const conn = await amqp.connect(amqpUrl);
    const chan = await conn.createChannel();
    return CFPubSubJobs.getAndRunTask(chan, q, job);
  },
};
module.exports = CFPubSubJobs;