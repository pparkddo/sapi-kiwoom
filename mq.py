from abc import ABC, abstractmethod
from threading import Thread
import json
import pika


TASK_SUCCEED = "TASK_SUCCEED"
TASK_FAILED = "TASK_FAILED"


class MessageQueueConsumer(ABC):

    @abstractmethod
    def callback(self, channel, method, properties, body):
        pass


def consume(broker_url, queue, callback):
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    generate_queue(channel, queue)
    channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
    channel.start_consuming()


def generate_queue(channel, queue):
    channel.queue_declare(queue=queue)


def publish(
        broker_url,
        body,
        queue,
        properties=None,
        exchange="",
        routing_key="",
        channel=None,
        is_queue_exist=True
    ):

    if not channel:
        connection = pika.BlockingConnection(pika.URLParameters(broker_url))
        channel = connection.channel()

    if not is_queue_exist:
        generate_queue(channel, queue)

    routing_key = routing_key if routing_key != "" else queue

    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=properties
    )

    if not channel:
        connection.close()


def get_consume_thread(broker_url, queue, callback):
    return Thread(target=consume, args=(broker_url, queue, callback))


def serialize(message):
    return json.dumps(message, ensure_ascii=False, default=str)


def deserialize(message):
    return json.loads(message)
