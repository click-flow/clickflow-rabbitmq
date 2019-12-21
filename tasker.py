import logging
import json
import pika
import time
import threading

from queue import Queue


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

RABBITMQ_EXCHANGE = 'cftask'


class Publisher:

    """
    Based on
    github.com/pika/pika/blob/master/examples/asynchronous_publisher_example.py
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

        self._connection = None
        self._channel = None

        self._stopping = False
        self._url = amqp_url

        self.queue = queue
        self.routing_key = routing_key or self.queue
        self.exchange = exchange
        self.exchange_type = exchange_type

        self._conn_control_thread = threading.Thread(target=self._connection_manager)
        self._q_control_thread = threading.Thread(target=self._queue_manager)
        self._message_queue = Queue()

    def _connection_manager(self):
        LOGGER.info('Connection manager thread starting...')
        while not self._stopping:
            self._connection = None
            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except Exception as e:
                LOGGER.error('Connection failed: %s', e)
                self.stop()
        LOGGER.info('Connection manager thread stopped')

    def _queue_manager(self):
        LOGGER.info('Message queue manager thread starting...')
        while not self._stopping:
            message = self._message_queue.get()
            self._publish_message(message)
        LOGGER.info('Message queue manager thread stopped')

    def connect(self):
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self, _unused_connection):
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: %s',
                           reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Channel %i opened', channel)
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=self.exchange_type,
            callback=self.on_exchange_declareok)

    def on_channel_closed(self, channel, reason):
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self._channel = None
        if not self._stopping:
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
            callback=self.on_queue_bindok)

    def on_queue_bindok(self, _unused_frame):
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        pass

    def publish_message(self, message):
        """ Public interface to send messages to queue
        """
        self._message_queue.put(message)

    def _publish_message(self, message):
        if self._channel is None or not self._channel.is_open:
            return

        payload = json.dumps(message, ensure_ascii=False)
        self._channel.basic_publish(
            self.exchange,
            self.routing_key,
            payload,
            pika.BasicProperties(content_type='application/json'),
        )
        LOGGER.info('Published message: %s', payload)

    def start(self):
        self._q_control_thread.start()
        self._conn_control_thread.start()

    def stop(self):
        LOGGER.info('Stopping Publisher')
        self._stopping = True
        self.close_channel()
        self.close_connection()
        LOGGER.info('Publisher stopped')
        # TODO figure this out
        # if self._connection is not None and not self._connection.is_closed:
        #     # Finish closing
        #     self._connection.ioloop.start()

    def close_channel(self):
        if self._channel is not None:
            LOGGER.info('Closing the channel')
            self._channel.close()

    def close_connection(self):
        if self._connection is not None:
            LOGGER.info('Closing connection')
            self._connection.close()

def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    amqp_url = 'amqp://rabbitmq:cfrmq9988@localhost:5672'
    publisher = Publisher(amqp_url, 'cftask')
    publisher.start()
    time.sleep(2)
    publisher.publish_message({'thisis': 'a test from Python'})
    time.sleep(2)
    publisher.stop()


if __name__ == '__main__':
    main()
