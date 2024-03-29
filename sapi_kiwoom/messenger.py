from datetime import datetime
from dataclasses import dataclass

from .mq import publish, serialize, deserialize, generate_queue, get_connection, get_channel
from .utils import get_task_response


TASK_SUCCEED = "TASK_SUCCEED"
TASK_FAILED = "TASK_FAILED"
DEFAULT_REQUEST_QUEUE_NAME = "tasks"
DEFAULT_RESPONSE_QUEUE_NAME = "sapi-kiwoom"


class MessageParsingError(Exception):
    pass


@dataclass
class Message:

    task_id: str
    method: str
    parameters: dict
    request_time: str


def get_success_message(task_id, message):
    return get_task_response(
        task_id,
        message,
        datetime.now(),
        TASK_SUCCEED
    )


def get_fail_message(task_id, message):
    return get_task_response(
        task_id,
        message,
        datetime.now(),
        TASK_FAILED
    )


def get_message(body):
    try:
        deserialized = deserialize(body)
        return Message(
            task_id=deserialized["task_id"],
            method=deserialized["method"],
            parameters=deserialized["parameters"],
            request_time=deserialized["request_time"],
        )
    except Exception as error:
        raise MessageParsingError("Error occurred in parsing message") from error


class Messenger:

    def __init__(self, broker_url):
        self.broker_url = broker_url
        self.delivery_tags = {}
        self.reply_queues = {}
        self.channel = None
        self.setup_default_queue()

    def setup_default_queue(self):
        connection = get_connection(self.broker_url)
        channel = get_channel(connection)
        generate_queue(channel, DEFAULT_RESPONSE_QUEUE_NAME)

    def get_broker_url(self):
        return self.broker_url

    def send(self, task_response, reply_queue, channel):
        publish(self.broker_url, serialize(task_response), reply_queue, channel=channel)

    def acknowledge(self, channel, delivery_tag):
        channel.basic_ack(delivery_tag=delivery_tag)

    def generate_delivery_tag(self, method):
        return method.delivery_tag

    def generate_reply_queue(self, properties):
        return properties.reply_to if properties.reply_to else DEFAULT_RESPONSE_QUEUE_NAME

    def parse_message(self, channel, method, properties, body):
        message = get_message(body)
        self._set_message_properties(message.task_id, channel, method, properties)
        return message

    def acknowledge_message(self, task_id):
        self.acknowledge(self.channel, self._pop_delivery_tag(task_id))

    def send_success_message(self, task_id, message, pop_reply_queue=True, ack=True):
        task_response = get_success_message(task_id, message)
        self._send_message(task_response, pop_reply_queue, ack)

    def send_fail_message(self, task_id, message, pop_reply_queue=True, ack=True):
        task_response = get_fail_message(task_id, message)
        self._send_message(task_response, pop_reply_queue, ack)

    def _set_message_properties(self, task_id, channel, method, properties):
        delivery_tag = self.generate_delivery_tag(method)
        self.delivery_tags[task_id] = delivery_tag

        reply_queue = self.generate_reply_queue(properties)
        self.reply_queues[task_id] = reply_queue

        if not self.channel:
            self.channel = channel

    def _get_reply_queue(self, task_id):
        return self.reply_queues.get(task_id)

    def _pop_reply_queue(self, task_id):
        return self.reply_queues.pop(task_id)

    def _pop_delivery_tag(self, task_id):
        return self.delivery_tags.pop(task_id)

    def _send_message(self, task_response, pop_reply_queue, ack):
        task_id = task_response["task_id"]
        reply_queue = (
            self._pop_reply_queue(task_id) if pop_reply_queue
            else self._get_reply_queue(task_id)
        )
        self.send(task_response, reply_queue, channel=self.channel)
        if ack:
            self.acknowledge_message(task_id)
