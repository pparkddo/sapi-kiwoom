from datetime import datetime
from dataclasses import dataclass

from mq import publish, serialize, deserialize, TASK_SUCCEED, TASK_FAILED
from utils import get_task_response


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

    def __init__(self):
        self.delivery_tags = {}
        self.reply_queues = {}
        self.channel = None

    def publish(self, task_response, reply_queue, channel):
        publish(serialize(task_response), reply_queue, channel=channel)

    def acknowledge(self, channel, delivery_tag):
        channel.basic_ack(delivery_tag=delivery_tag)

    def get_delivery_tag(self, method):
        return method.delivery_tag

    def get_reply_queue(self, properties):
        return properties.reply_to if properties.reply_to else self._get_default_reply_queue()

    def parse_message(self, channel, method, properties, body):
        message = get_message(body)
        self._set_message_properties(message.task_id, channel, method, properties)
        return message

    def publish_success_message(self, task_id, message):
        task_response = get_success_message(task_id, message)
        self._publish_response(task_response)

    def publish_fail_message(self, task_id, message):
        task_response = get_fail_message(task_id, message)
        self._publish_response(task_response)

    def acknowledge_task(self, task_id):
        self.acknowledge(self.channel, self._pop_delivery_tag(task_id))

    def send_success_message(self, task_id, message):
        task_response = get_success_message(task_id, message)
        self._send(task_response)

    def send_fail_message(self, task_id, message):
        task_response = get_fail_message(task_id, message)
        self._send(task_response)

    def _set_message_properties(self, task_id, channel, method, properties):
        delivery_tag = self.get_delivery_tag(method)
        self.delivery_tags.update({task_id: delivery_tag})

        reply_queue = self.get_reply_queue(properties)
        self.reply_queues.update({task_id: reply_queue})

        if not self.channel:
            self.channel = channel

    def _get_default_reply_queue(self):
        return "sapi-kiwoom"

    def _get_reply_queue(self, task_id):
        return self.reply_queues.get(task_id)

    def _pop_reply_queue(self, task_id):
        return self.reply_queues.pop(task_id)

    def _pop_delivery_tag(self, task_id):
        return self.delivery_tags.pop(task_id)

    def _publish_response(self, task_response):
        task_id = task_response["task_id"]
        self.publish(task_response, self._get_reply_queue(task_id), channel=self.channel)

    def _send(self, task_response):
        task_id = task_response["task_id"]
        self.publish(task_response, self._pop_reply_queue(task_id), channel=self.channel)
        self.acknowledge_task(task_id)
