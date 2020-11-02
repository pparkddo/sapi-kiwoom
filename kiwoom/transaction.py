from dataclasses import dataclass
from random import randrange

from .method import REQUEST_DAY_CANDLE, REQUEST_MINUTE_CANDLE


# Kiwoom Request Continuous Status
KIWOOM_SINGLE_REQUEST = "0"
KIWOOM_CONTINUE_REQUEST = "2"


# Kiwoom Transaction Request Status
REQUEST_SUCCEED = 0


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


def get_transaction_code(method):
    code = KIWOOM_TRANSACTION_CODE_MAP.get(method)
    if code is None:
        raise ValueError(f"Wrong or unsupported method: {method}")
    return code


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



def get_randomized_screen_number():
    return f"{randrange(1, 200):>04d}"


class KiwoomTransactionRequest:
    def __init__(self, task_id, method, parameters=None):
        self.transaction_id = task_id
        self.transaction_code = get_transaction_code(method)
        self.transaction_parameters = get_transaction_parameters(self.transaction_code, parameters)
        self.continuous = KIWOOM_SINGLE_REQUEST
        self.screen_no = get_randomized_screen_number()


def is_last_transaction_data(has_next):
    return has_next == KIWOOM_SINGLE_REQUEST
