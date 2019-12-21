import logging
import json
import pika
import time


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

RABBITMQ_EXCHANGE = 'cftask'


class Consumer:

    """
    Based on:
    github.com/pika/pika/blob/master/examples/asynchronous_consumer_example.py
    """

    def __init__(
        self,
        amqp_url,
        queue,
        routing_key=None,
        exchange=RABBITMQ_EXCHANGE,
        exchange_type='topic',
        on_message=None,
    ):

        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._consuming = False

        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

        self.queue = queue
        self.routing_key = routing_key or self.queue
        self.exchange = exchange
        self.exchange_type = exchange_type
        self._on_message_callback = on_message

    def connect(self):
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False

        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=self.exchange_type,
            callback=self.on_exchange_declareok)

    def on_channel_closed(self, channel, reason):
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def on_exchange_declareok(self, _unused_frame):
        self._channel.queue_declare(
            queue=self.queue, callback=self.on_queue_declareok, durable=True)

    def on_queue_declareok(self, _unused_frame):
        LOGGER.info('Binding %s to %s with %s', self.exchange, self.queue,
                    self.routing_key)
        self._channel.queue_bind(
            self.queue,
            self.exchange,
            routing_key=self.routing_key,
            callback=self.on_bindok)

    def on_bindok(self, _unused_frame):
        LOGGER.info('Queue bound: %s', self.queue)
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.
        """
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        self.start_consuming()

    def start_consuming(self):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(
            self.queue, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)
        if self._on_message_callback is not None:
            self._on_message_callback(body)

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self._consumer_tag, self.on_cancelok)

    def on_cancelok(self, _unused_frame):
        self._consuming = False
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info('Stopped')


class ReconnectingConsumer:

    def __init__(
        self,
        amqp_url,
        queue,
        routing_key=None,
        exchange=RABBITMQ_EXCHANGE,
        exchange_type='topic',
        on_message=None,
    ):
        self._reconnect_delay = 0
        self._url = amqp_url

        self.queue = queue
        self.routing_key = routing_key or self.queue
        self.exchange = exchange
        self.exchange_type = exchange_type
        self._on_message_callback = on_message

        self._consumer = self._get_consumer()

    def _get_consumer(self):
        return Consumer(
            self._url,
            self.queue,
            self.routing_key,
            self.exchange,
            self.exchange_type,
            self._on_message_callback,
        )

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = self._get_consumer()

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def on_message(message):
    LOGGER.info('Got message: %s', message)


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    amqp_url = 'amqp://rabbitmq:cfrmq9988@localhost:5672'
    consumer = ReconnectingConsumer(amqp_url, 'cftask', on_message=on_message)
    consumer.run()


if __name__ == '__main__':
    main()
