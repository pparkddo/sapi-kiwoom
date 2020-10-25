from abc import ABC, abstractmethod
from threading import Thread
import json
import pika


BROKER_URL = "amqp://localhost:5672"

TASK_SUCCEED = "TASK_SUCCEED"
TASK_FAILED = "TASK_FAILED"


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


def get_consume_thread(queue, callback):
    return Thread(target=consume, args=(queue, callback))


def serialize(message):
    return json.dumps(message, ensure_ascii=False, default=str)


def deserialize(message):
    return json.loads(message)
