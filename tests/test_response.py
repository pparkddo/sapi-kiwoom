import subprocess
import sys
import time
import unittest
from datetime import datetime

import pika

from sapi_kiwoom.mq import publish, get_connection, get_channel, serialize, deserialize
from sapi_kiwoom.messenger import TASK_SUCCEED


def wait_util_server_process_log_in():
    time.sleep(100)


def get_task_request(task_id, method, request_time, parameters=None):
    return {
        "task_id": task_id,
        "method": method,
        "parameters": parameters,
        "request_time": request_time,
    }


def generate_property(reply_to):
    return pika.BasicProperties(reply_to=reply_to)


class MessageQueueConnectionTest(unittest.TestCase):

    def test_connection(self):
        test_broker_url = "amqp://localhost:5672"

        connection = get_connection(test_broker_url)
        self.assertIsInstance(connection, pika.BlockingConnection)

        channel = get_channel(connection)
        self.assertIsNotNone(channel)


class ResponseTest(unittest.TestCase):

    def setUp(self):
        self.test_broker_url = "amqp://localhost:5672"
        self.connection = get_connection(self.test_broker_url)
        self.channel = get_channel(self.connection)
        self.request_queue = "tasks"
        command = [sys.executable, "-m", "sapi_kiwoom", self.test_broker_url]
        self.server_process = subprocess.Popen(command, stdout=subprocess.PIPE)
        wait_util_server_process_log_in()

    def test_response(self):
        parameters = {
            "stock_code": "015760",
        }
        now = datetime.now()
        test_task_id = "test-request"
        task_request = get_task_request(
            test_task_id,
            "get-stock-name",
            now,
            parameters,
        )

        request_body = serialize(task_request)
        response_queue = "test-response-queue"
        self.channel.queue_declare(response_queue, exclusive=True)
        publish(
            self.test_broker_url,
            request_body,
            self.request_queue,
            properties=generate_property(response_queue),
            channel=self.channel
        )

        messages = []
        self.channel.basic_consume(
            queue=response_queue,
            on_message_callback=lambda *args: messages.append(args),
            auto_ack=True
        )

        while not messages:
            self.connection.process_data_events(time_limit=10)

        self.channel.queue_delete(response_queue)

        response_body = deserialize(messages[0][-1])
        self.assertEqual(test_task_id, response_body["task_id"])
        self.assertEqual(TASK_SUCCEED, response_body["status"])

    def tearDown(self):
        self.server_process.kill()
