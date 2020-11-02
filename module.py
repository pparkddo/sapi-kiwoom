from datetime import datetime
from dataclasses import dataclass
from random import randrange

from PyQt5.QAxContainer import QAxWidget

from mq import publish, serialize, deserialize, TASK_SUCCEED, TASK_FAILED, get_consume_thread
from utils import get_task_response, get_task_id
from delay import wait_until_request_available


def get_randomized_screen_number():
    return f"{randrange(1, 200):>04d}"


# Method
REQUEST_MINUTE_CANDLE = "request-minute-candle"
REQUEST_DAY_CANDLE = "request-day-candle"
GET_STOCK_NAME = "get-stock-name"


# Method type
TRANSACTION = "TRANSACTION"
REALTIME = "REALTIME"
LOOKUP = "LOOKUP"


METHOD_TYPE_MAP = {
    REQUEST_MINUTE_CANDLE: TRANSACTION,
    REQUEST_DAY_CANDLE: TRANSACTION,
    GET_STOCK_NAME: LOOKUP,
}


def get_method_type(method):
    if method in METHOD_TYPE_MAP:
        return METHOD_TYPE_MAP[method]
    else:
        raise KeyError(f"Key{method} is not exists in method map")


@dataclass
class KiwoomLookupParameter:
    name: str
    description: str


KIWOOM_LOOKUP_PARAMETER_MAP = {
    GET_STOCK_NAME: [
        KiwoomLookupParameter("stock_code", "6자리 종목코드"),
    ],
}


def get_lookup_parameters(method, parameters):
    lookup_parameters = KIWOOM_LOOKUP_PARAMETER_MAP[method]
    return [parameters[each.name] for each in lookup_parameters]


@dataclass
class KiwoomTransactionParameter:
    origin_name: str
    changed_name: str
    description: str


@dataclass
class KiwoomTransactionResultField:
    origin_name: str
    changed_name: str


# Transaction Code
REQUEST_MINUTE_CANDLE_CODE = "OPT10080"
REQUEST_DAY_CANDLE_CODE = "OPT10081"


KIWOOM_TRANSACTION_CODE_MAP = {
    REQUEST_MINUTE_CANDLE: REQUEST_MINUTE_CANDLE_CODE,
    REQUEST_DAY_CANDLE: REQUEST_DAY_CANDLE_CODE,
}


def get_transaction_code(method):
    code = KIWOOM_TRANSACTION_CODE_MAP.get(method)
    if code is None:
        raise ValueError(f"Wrong or unsupported method: {method}")
    return code


KIWOOM_TRANSACTION_PARAMETER_MAP = {
    REQUEST_MINUTE_CANDLE_CODE: [
        KiwoomTransactionParameter("종목코드", "stock_code", "6자리 종목코드"),
        KiwoomTransactionParameter(
            "틱범위",
            "tick",
            "1:1분, 3:3분, 5:5분, 10:10분, 15:15분, 30:30분, 45:45분, 60:60분"
        ),
        KiwoomTransactionParameter(
            "수정주가구분",
            "is_adjusted",
            "0 or 1, 수신데이터 1:유상증자, 2:무상증자, 4:배당락, 8:액면분할, 16:액면병합, 32:기업합병, 64:감자, 256:권리락"
        ),
    ],
    REQUEST_DAY_CANDLE_CODE: [
        KiwoomTransactionParameter("종목코드", "stock_code", "6자리 종목코드"),
        KiwoomTransactionParameter("기준일자", "to", "조회할 마지막 날짜(YYYYMMDD)"),
        KiwoomTransactionParameter(
            "수정주가구분",
            "is_adjusted",
            "0 or 1, 수신데이터 1:유상증자, 2:무상증자, 4:배당락, 8:액면분할, 16:액면병합, 32:기업합병, 64:감자, 256:권리락"
        ),
    ],
}


KIWOOM_TRANSACTION_RESPONSE_FIELD_MAP = {
    REQUEST_MINUTE_CANDLE_CODE: [
        KiwoomTransactionResultField("현재가", "closing"),
        KiwoomTransactionResultField("거래량", "volume"),
        KiwoomTransactionResultField("체결시간", "timestamp"),
        KiwoomTransactionResultField("시가", "opening"),
        KiwoomTransactionResultField("고가", "high"),
        KiwoomTransactionResultField("저가", "low"),
        KiwoomTransactionResultField("수정주가구분", "is_adjusted"),
        KiwoomTransactionResultField("수정비율", "adjust_rate"),
        KiwoomTransactionResultField("대업종구분", "main_sector"),
        KiwoomTransactionResultField("소업종구분", "sub_sector"),
        KiwoomTransactionResultField("종목정보", "stock_info"),
        KiwoomTransactionResultField("수정주가이벤트", "adjust_event"),
        KiwoomTransactionResultField("전일종가", "previous_closing"),
    ],
    REQUEST_DAY_CANDLE_CODE: [
        KiwoomTransactionResultField("종목코드", "stock_code"),
        KiwoomTransactionResultField("현재가", "closing"),
        KiwoomTransactionResultField("거래량", "volume"),
        KiwoomTransactionResultField("거래대금", "tr_amount"),
        KiwoomTransactionResultField("일자", "day"),
        KiwoomTransactionResultField("시가", "opening"),
        KiwoomTransactionResultField("고가", "high"),
        KiwoomTransactionResultField("저가", "low"),
        KiwoomTransactionResultField("수정주가구분", "is_adjusted"),
        KiwoomTransactionResultField("수정비율", "adjust_rate"),
        KiwoomTransactionResultField("대업종구분", "main_sector"),
        KiwoomTransactionResultField("소업종구분", "sub_sector"),
        KiwoomTransactionResultField("종목정보", "stock_info"),
        KiwoomTransactionResultField("수정주가이벤트", "adjust_event"),
        KiwoomTransactionResultField("전일종가", "previous_closing"),
    ],
}


def get_transaction_parameters(transaction_code, parameters):
    kiwoom_transaction_parameters = KIWOOM_TRANSACTION_PARAMETER_MAP[transaction_code]

    transaction_parameters = {}

    for each in kiwoom_transaction_parameters:
        if each.changed_name not in parameters:
            raise ValueError(f"{each.origin_name}({each.changed_name}) is essential")

        value = parameters[each.changed_name]

        transaction_parameters.update({each.origin_name: value})
    return transaction_parameters


def is_empty_transaction_data(transaction_data):
    return transaction_data is None


def get_transaction_response(transaction_code, transaction_data):
    if is_empty_transaction_data(transaction_data):
        return []
    fields = KIWOOM_TRANSACTION_RESPONSE_FIELD_MAP[transaction_code]
    changed_fields = [each.changed_name for each in fields]
    return [dict(zip(changed_fields, row)) for row in transaction_data]


@dataclass
class KiwoomTaskParameter:
    name: str
    description: str


# Kiwoom Task Parameter
KIWOOM_TASK_PARAMETER_MAP = {
    REQUEST_MINUTE_CANDLE: [
        KiwoomTaskParameter("from", "조회를 시작할 기간(YYYYMMDD)"),
        KiwoomTaskParameter("to", "조회할 마지막 기간(YYYYMMDD)"),
    ],
    REQUEST_DAY_CANDLE: [
        KiwoomTaskParameter("from", "조회를 시작할 기간(YYYYMMDD)"),
    ],
}


def validate_task_parameters(method, parameters):
    kiwoom_task_parameters = KIWOOM_TASK_PARAMETER_MAP[method]

    for each in kiwoom_task_parameters:
        if each.name not in parameters:
            raise ValueError(f"Task parameter '{each.name}'({each.description}) is missed")


# Task Status
PENDING = "PENDING"
REQUESTED = "REQUESTED"
COMPLETED = "COMPLETED"
FAILED = "FAILED"


class KiwoomTask:
    def __init__(self, message):
        self.task_id = message["task_id"]
        self.method = message["method"]
        self.parameters = message["parameters"]
        self.request_time = message["request_time"]
        self.response_time = None
        self.transaction_code = None
        self.status = PENDING
        self.transaction_request = None
        self.transaction_responses = []

    @property
    def last_response(self):
        return self.transaction_responses[-1]

    @property
    def has_result(self):
        return self.transaction_responses

    @property
    def is_completed(self):
        transaction_code = self.transaction_code
        parameters = self.parameters
        last_response = self.last_response

        if transaction_code == REQUEST_DAY_CANDLE_CODE:
            return last_response["day"] <= parameters["from"]
        elif transaction_code == REQUEST_MINUTE_CANDLE_CODE:
            return last_response["timestamp"] <= parameters["from"]

    @property
    def filtered_responses(self):
        transaction_code = self.transaction_code
        parameters = self.parameters
        transaction_responses = self.transaction_responses

        if transaction_code == REQUEST_DAY_CANDLE_CODE:
            return list(
                filter(
                    lambda x: parameters["from"] <= x["day"] <= parameters["to"],
                    transaction_responses
                )
            )
        elif transaction_code == REQUEST_MINUTE_CANDLE_CODE:
            return list(
                filter(
                    lambda x: parameters["from"] <= x["timestamp"] <= parameters["to"],
                    transaction_responses
                )
            )


# Kiwoom Request Continuous Status
KIWOOM_SINGLE_REQUEST = "0"
KIWOOM_CONTINUE_REQUEST = "2"


class KiwoomTransactionRequest:
    def __init__(self, task_id, method, parameters=None):
        self.transaction_id = task_id
        self.transaction_code = get_transaction_code(method)
        self.transaction_parameters = get_transaction_parameters(self.transaction_code, parameters)
        self.continuous = KIWOOM_SINGLE_REQUEST
        self.screen_no = get_randomized_screen_number()


# Kiwoom Transaction Request Status
REQUEST_SUCCEED = 0


# Kiwoom Connection Status
CONNECTION_SUCCEED = 0


def is_last_transaction_data(has_next):
    return has_next == KIWOOM_SINGLE_REQUEST


def get_message(body):
    return deserialize(body)


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
        self.channels = {}

        self.request_timestamps = []

        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

        self.OnEventConnect.connect(self.on_connect)
        self.OnReceiveMsg.connect(self.on_receive_message)
        self.OnReceiveTrData.connect(self.on_receive_tr_data)

    def acknowledge_task(self, task_id):
        channel = self.channels[task_id]
        delivery_tag = self.delivery_tags[task_id]
        channel.basic_ack(delivery_tag=delivery_tag)

    def start_consuming(self):
        return self.consumer.start()

    def callback(self, channel, method, properties, body):
        # pylint: disable=unused-argument
        message = get_message(body)
        task_id = message["task_id"]
        self.delivery_tags.update({task_id: method.delivery_tag})
        self.channels.update({task_id: channel})
        self.consume_task_request(message)

    def consume_task_request(self, message):
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
            publish(serialize(task_response), "sapi-kiwoom")
            return

        if method_type == REALTIME:
            self.acknowledge_task(task_id)
        elif method_type == LOOKUP:
            lookup_result = self.get_lookup_result(method, parameters)
            task_response = get_task_response(
                task_id,
                lookup_result,
                datetime.now(),
                TASK_SUCCEED
            )
            publish(serialize(task_response), "sapi-kiwoom")
            self.acknowledge_task(task_id)
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
                publish(serialize(task_response), "sapi-kiwoom")
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
                publish(serialize(task_response), "sapi-kiwoom")
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
        if error_code == CONNECTION_SUCCEED:
            task_id = get_task_id()
            task_response = get_task_response(
                task_id,
                "Connection Success",
                datetime.now(),
                TASK_SUCCEED
            )
            publish(serialize(task_response), "sapi-kiwoom")
            self.start_consuming()
        else:
            task_response = get_task_response(
                task_id,
                "Connection Failed",
                datetime.now(),
                TASK_FAILED
            )
            publish(serialize(task_response), "sapi-kiwoom")

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
            publish(serialize(task_response), "sapi-kiwoom")
            self.acknowledge_task(task_id)
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
            publish(serialize(task_response), "sapi-kiwoom")

    def get_transaction_data(self, transcation_code, task_id):
        return self.dynamicCall("GetCommDataEx(QString, QString)", transcation_code, task_id)

    def get_lookup_result(self, method, parameters):
        lookup_paramters = get_lookup_parameters(method, parameters)
        if method == GET_STOCK_NAME:
            return self.get_stock_name(lookup_paramters)

    def get_stock_name(self, issue_code):
        return self.dynamicCall("GetMasterCodeName(QString)", issue_code)
