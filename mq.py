from abc import ABC, abstractmethod
import json
import pika


BROKER_URL = "amqp://localhost:5672"


class MessageQueueConsumer(ABC):

    @abstractmethod
    def callback(self, channel, method, properties, body):
        pass


def consume(queue, callback):
    connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


def publish(body, queue, exchange="", routing_key=""):
    connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    routing_key = routing_key if routing_key != "" else queue

    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
    connection.close()


def serialize(message):
    return json.dumps(message, default=str)


def deserialize(message):
    return json.loads(message)
