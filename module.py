from datetime import datetime
from dataclasses import dataclass
from random import randrange
from typing import Any, Union, List

from PyQt5.QAxContainer import QAxWidget

from mq import publish, serialize, deserialize, TASK_SUCCEED, TASK_FAILED, get_consume_thread
from utils import get_task_response, get_task_id
from delay import wait_until_request_available


def get_randomized_screen_number():
    return f"{randrange(1, 200):>04d}"


REQUEST_DAY_CANDLE = "OPT10081"


@dataclass
class KiwoomTransactionParameter:
    origin_name: str
    changed_name: str
    avaliable: Union[List[Any], str]
    default: Any = None


@dataclass
class KiwoomTransactionResultField:
    origin_name: str
    changed_name: str


KIWOOM_TRANSACTION_CODE_MAP = {
    "day-candle": REQUEST_DAY_CANDLE,
}


def get_transaction_code(method):
    code = KIWOOM_TRANSACTION_CODE_MAP.get(method)
    if code is None:
        raise ValueError(f"Wrong or unsupported method: {method}")
    return code


KIWOOM_TRANSACTION_PARAMETER_MAP = {
    REQUEST_DAY_CANDLE: [
        KiwoomTransactionParameter("종목코드", "stock_code", "6자리 종목코드"),
        KiwoomTransactionParameter("기준일자", "to", "조회할 마지막 날짜(YYYYMMDD)"),
        KiwoomTransactionParameter(
            "수정주가구분",
            "is_revised",
            "0 or 1, 수신데이터 1:유상증자, 2:무상증자, 4:배당락, 8:액면분할, 16:액면병합, 32:기업합병, 64:감자, 256:권리락",
            "0"
        ),
    ],
}


KIWOOM_TRANSACTION_RESPONSE_FIELD_MAP = {
    REQUEST_DAY_CANDLE: [
        KiwoomTransactionResultField("종목코드", "stock_code"),
        KiwoomTransactionResultField("현재가", "closing"),
        KiwoomTransactionResultField("거래량", "volume"),
        KiwoomTransactionResultField("거래대금", "tr_amount"),
        KiwoomTransactionResultField("일자", "day"),
        KiwoomTransactionResultField("시가", "opening"),
        KiwoomTransactionResultField("고가", "high"),
        KiwoomTransactionResultField("저가", "low"),
        KiwoomTransactionResultField("수정주가구분", "is_revised"),
        KiwoomTransactionResultField("수정비율", "revise_rate"),
        KiwoomTransactionResultField("대업종구분", "main_sector"),
        KiwoomTransactionResultField("소업종구분", "sub_sector"),
        KiwoomTransactionResultField("종목정보", "stock_info"),
        KiwoomTransactionResultField("수정주가이벤트", "revise_event"),
        KiwoomTransactionResultField("전일종가", "previous_closing"),
    ],
}


def get_transaction_parameters(transaction_code, parameters):
    kiwoom_transaction_parameters = KIWOOM_TRANSACTION_PARAMETER_MAP[transaction_code]

    transaction_parameters = {}

    for each in kiwoom_transaction_parameters:
        value = parameters.get(each.changed_name, each.default)

        if not value:
            raise ValueError(f"{each.origin_name}({each.changed_name}) is essential")
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
        if self.transaction_code == REQUEST_DAY_CANDLE:
            return self.last_response["day"] <= self.parameters["from"]

    @property
    def filtered_responses(self):
        if self.transaction_code == REQUEST_DAY_CANDLE:
            return list(
                filter(
                    lambda x: self.parameters["from"] <= x["day"] <= self.parameters["to"],
                    self.transaction_responses
                )
            )


KIWOOM_SINGLE_REQUEST = "0"
KIWOOM_CONTINUE_REQUEST = "2"


class KiwoomTransactionRequest:
    def __init__(self, task_id, method, parameters=None):
        self.transaction_id = task_id
        self.transaction_code = get_transaction_code(method)
        self.transaction_parameters = get_transaction_parameters(self.transaction_code, parameters)
        self.continuous = KIWOOM_SINGLE_REQUEST
        self.screen_no = get_randomized_screen_number()


REQUEST_SUCCEED = 0


CONNECTION_SUCCEED = 0


def is_last_transaction_data(has_next):
    return has_next == KIWOOM_SINGLE_REQUEST


class KiwoomModule(QAxWidget):

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self):
        super().__init__()
        self.tasks = {}
        self.request_timestamps = []
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        self.OnEventConnect.connect(self.on_connect)
        self.OnReceiveMsg.connect(self.on_receive_message)
        self.OnReceiveTrData.connect(self.on_receive_tr_data)
        self.consumer = get_consume_thread("tasks", self.callback)

    def callback(self, channel, method, properties, body):
        # pylint: disable=unused-argument
        self.consume_task_request(deserialize(body))

    def consume_task_request(self, message):
        task_id = message["task_id"]
        if message["method"] == "realtime":
            pass
        else:
            task = KiwoomTask(message)
            transaction_request = KiwoomTransactionRequest(
                task_id,
                message["method"],
                message["parameters"]
            )
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
