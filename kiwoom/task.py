from dataclasses import dataclass

from .method import (
    REQUEST_DAY_CANDLE,
    REQUEST_MINUTE_CANDLE,
    REQUEST_UPPER_AND_LOW,
    REQUEST_OFFER_PRICE_INFO,
    REQUEST_OFFHOUR_SINGLE_TRADE_INFO,
    REQUEST_SHORT_TREND,
)
from .transaction import (
    REQUEST_DAY_CANDLE_CODE,
    REQUEST_MINUTE_CANDLE_CODE,
    REQUEST_UPPER_AND_LOW_CODE,
    REQUEST_OFFER_PRICE_INFO_CODE,
    REQUEST_OFFHOUR_SINGLE_TRADE_INFO_CODE,
    REQUEST_SHORT_TREND_CODE,
)


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
    REQUEST_UPPER_AND_LOW: [],
    REQUEST_OFFER_PRICE_INFO: [],
    REQUEST_OFFHOUR_SINGLE_TRADE_INFO: [],
    REQUEST_SHORT_TREND: [],
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
        elif transaction_code == REQUEST_UPPER_AND_LOW_CODE:
            return True
        elif transaction_code == REQUEST_OFFER_PRICE_INFO_CODE:
            return True
        elif transaction_code == REQUEST_OFFHOUR_SINGLE_TRADE_INFO_CODE:
            return True
        elif transaction_code == REQUEST_SHORT_TREND_CODE:
            return True

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
        elif transaction_code == REQUEST_UPPER_AND_LOW_CODE:
            return transaction_responses
        elif transaction_code == REQUEST_OFFER_PRICE_INFO_CODE:
            return transaction_responses
        elif transaction_code == REQUEST_OFFHOUR_SINGLE_TRADE_INFO_CODE:
            return transaction_responses
        elif transaction_code == REQUEST_SHORT_TREND_CODE:
            return transaction_responses
