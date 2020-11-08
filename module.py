from datetime import datetime

from PyQt5.QAxContainer import QAxWidget

from mq import publish, serialize, deserialize, TASK_SUCCEED, TASK_FAILED, get_consume_thread
from utils import get_task_response, get_task_id
from delay import wait_until_request_available
from kiwoom.method import (
    get_method_type,
    REALTIME,
    LOOKUP,
    GET_STOCK_NAME,
    GET_STOCK_CODES,
    GET_STOCK_STATES,
)
from kiwoom.lookup import get_lookup_parameters
from kiwoom.transaction import (
    KiwoomTransactionRequest,
    get_transaction_response,
    is_last_transaction_data,
    KIWOOM_CONTINUE_REQUEST,
    REQUEST_SUCCEED,
)
from kiwoom.task import validate_task_parameters, KiwoomTask, REQUESTED, FAILED


# Kiwoom Connection Status
CONNECTION_SUCCEED = 0


def get_message(body):
    return deserialize(body)


def get_default_queue_dict(default):
    # pylint: disable=import-outside-toplevel
    from collections import defaultdict
    return defaultdict(lambda: default)


class KiwoomModule(QAxWidget):

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self):
        super().__init__()

        self.tasks = {}

        self.consumer = get_consume_thread("tasks", self.callback)
        self.delivery_tags = {}
        self.reply_queues = get_default_queue_dict("sapi-kiwoom")
        self.channel = None

        self.request_timestamps = []

        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

        self.OnEventConnect.connect(self.on_connect)
        self.OnReceiveMsg.connect(self.on_receive_message)
        self.OnReceiveTrData.connect(self.on_receive_tr_data)

    def acknowledge_task(self, task_id):
        delivery_tag = self.delivery_tags.pop(task_id)
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def get_reply_queue(self, task_id):
        return self.reply_queues[task_id]

    def publish_to_reply_queue(self, task_response):
        task_id = task_response["task_id"]
        publish(serialize(task_response), self.get_reply_queue(task_id), channel=self.channel)

    def publish_and_ack(self, task_response):
        task_id = task_response["task_id"]
        self.publish_to_reply_queue(task_response)
        self.acknowledge_task(task_id)

    def start_consuming(self):
        return self.consumer.start()

    def callback(self, channel, method, properties, body):
        # pylint: disable=unused-argument
        message = get_message(body)
        task_id = message["task_id"]

        self.delivery_tags.update({task_id: method.delivery_tag})

        reply_queue = properties.reply_to
        if reply_queue:
            self.reply_queues.update({task_id: reply_queue})

        if not self.channel:
            self.channel = channel

        self.handle_task_request(message)

    def handle_task_request(self, message):
        task_id = message["task_id"]
        method = message["method"]
        parameters = message["parameters"]

        try:
            method_type = get_method_type(method)
        except KeyError as error:
            error_message = str(error)
            task_response = get_task_response(
                task_id,
                error_message,
                datetime.now(),
                TASK_FAILED
            )
            self.publish_and_ack(task_response)
            return

        if method_type == REALTIME:
            self.acknowledge_task(task_id)
        elif method_type == LOOKUP:
            try:
                lookup_result = self.get_lookup_result(method, parameters)
            except (KeyError, TypeError) as error:
                error_message = str(error)
                task_response = get_task_response(
                    task_id,
                    error_message,
                    datetime.now(),
                    TASK_FAILED
                )
                self.publish_and_ack(task_response)
                return

            task_response = get_task_response(
                task_id,
                lookup_result,
                datetime.now(),
                TASK_SUCCEED
            )
            self.publish_and_ack(task_response)
        else:
            try:
                validate_task_parameters(method, parameters)
            except ValueError as error:
                error_message = str(error)
                task_response = get_task_response(
                    task_id,
                    error_message,
                    datetime.now(),
                    TASK_FAILED
                )
                self.publish_and_ack(task_response)
                return

            task = KiwoomTask(message)

            try:
                transaction_request = KiwoomTransactionRequest(
                    task_id,
                    method,
                    parameters
                )
            except ValueError as error:
                error_message = str(error)
                task_response = get_task_response(
                    task_id,
                    error_message,
                    datetime.now(),
                    TASK_FAILED
                )
                self.publish_and_ack(task_response)
                return

            task.transaction_code = transaction_request.transaction_code
            task.transaction_request = transaction_request
            self.tasks.update({task_id: task})
            self.request(transaction_request)

    def get_task(self, task_id):
        return self.tasks[task_id]

    def append_request_timestamps(self, request_timestamp):
        self.request_timestamps.append(request_timestamp)

    def on_connect(self, error_code):
        task_id = get_task_id()
        if error_code == CONNECTION_SUCCEED:
            task_response = get_task_response(
                task_id,
                "Connection Success",
                datetime.now(),
                TASK_SUCCEED
            )
            self.publish_to_reply_queue(task_response)
            self.start_consuming()
        else:
            task_response = get_task_response(
                task_id,
                "Connection Failed",
                datetime.now(),
                TASK_FAILED
            )
            self.publish_to_reply_queue(task_response)

    def on_receive_message(self, screen_no, tr_id, tr_code, message):
        print(
            f"screen_no: {screen_no}, tr_id: {tr_id}, tr_code: {tr_code}, message: {message}"
        )

    def on_receive_tr_data(
            self,
            screen_no,
            task_id,
            transaction_code,
            record_name,
            has_next,
            *deprecated
        ):
        # pylint: disable=unused-argument
        transaction_data = self.get_transaction_data(transaction_code, task_id)
        transaction_response = get_transaction_response(transaction_code, transaction_data)
        current_task = self.get_task(task_id)
        current_task.transaction_responses.extend(transaction_response)
        if (
                not current_task.has_result
                or current_task.is_completed
                or is_last_transaction_data(has_next)
            ):
            task_response = get_task_response(
                task_id,
                current_task.filtered_responses,
                datetime.now(),
                TASK_SUCCEED
            )
            self.publish_and_ack(task_response)
        else:
            current_task.transaction_request.continuous = KIWOOM_CONTINUE_REQUEST
            self.request(current_task.transaction_request)

    def connect(self):
        self.dynamicCall("CommConnect()")

    def set_transaction_parameter(self, key, value):
        self.dynamicCall("SetInputValue(QString, QString)", key, value)

    def set_transaction_parameters(self, transaction_parameters):
        for key, value in transaction_parameters.items():
            self.set_transaction_parameter(key, value)

    def request(self, transaction_request):
        wait_until_request_available(self.request_timestamps)
        self.append_request_timestamps(datetime.now())
        self.set_transaction_parameters(transaction_request.transaction_parameters)
        return_code = self.dynamicCall(
            "CommRqData(QString, QString, int, QString)",
            transaction_request.transaction_id,
            transaction_request.transaction_code,
            transaction_request.continuous,
            transaction_request.screen_no,
        )
        current_task = self.get_task(transaction_request.transaction_id)
        if return_code == REQUEST_SUCCEED:
            current_task.status = REQUESTED
        else:
            current_task.status = FAILED
            task_response = get_task_response(
                transaction_request.transaction_id,
                [],
                datetime.now(),
                TASK_FAILED
            )
            self.publish_and_ack(task_response)

    def get_transaction_data(self, transcation_code, task_id):
        return self.dynamicCall("GetCommDataEx(QString, QString)", transcation_code, task_id)

    def get_lookup_result(self, method, parameters):
        lookup_paramters = get_lookup_parameters(method, parameters)
        if method == GET_STOCK_NAME:
            return self.get_stock_name(lookup_paramters)
        elif method == GET_STOCK_CODES:
            return self.get_stock_codes(lookup_paramters)
        elif method == GET_STOCK_STATES:
            return self.get_stock_states(lookup_paramters)

    def get_stock_name(self, stock_code):
        return self.dynamicCall("GetMasterCodeName(QString)", stock_code)

    def get_stock_codes(self, market):
        return self.dynamicCall("GetCodeListByMarket(QString)", market)

    def get_stock_states(self, stock_code):
        return self.dynamicCall("GetMasterStockState(QString)", stock_code)
