from dataclasses import dataclass
from random import randrange

from .method import (
    REQUEST_DAY_CANDLE,
    REQUEST_MINUTE_CANDLE,
    REQUEST_UPPER_AND_LOW,
    REQUEST_OFFER_PRICE_INFO,
)


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
REQUEST_UPPER_AND_LOW_CODE = "OPT10017"
REQUEST_OFFER_PRICE_INFO_CODE = "OPT10004"


KIWOOM_TRANSACTION_CODE_MAP = {
    REQUEST_MINUTE_CANDLE: REQUEST_MINUTE_CANDLE_CODE,
    REQUEST_DAY_CANDLE: REQUEST_DAY_CANDLE_CODE,
    REQUEST_UPPER_AND_LOW: REQUEST_UPPER_AND_LOW_CODE,
    REQUEST_OFFER_PRICE_INFO: REQUEST_OFFER_PRICE_INFO_CODE,
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
    REQUEST_UPPER_AND_LOW_CODE: [
        KiwoomTransactionParameter("시장구분", "market", "000:전체, 001:코스피, 101:코스닥"),
        KiwoomTransactionParameter(
            "상하한구분",
            "query_type",
            "1:상한, 2:상승, 3:보합, 4: 하한, 5:하락, 6:전일상한, 7:전일하한"
        ),
        KiwoomTransactionParameter(
            "정렬구분",
            "sort_type",
            "1:종목코드순, 2:연속횟수순(상위100개), 3:등락률순"
        ),
        KiwoomTransactionParameter(
            "종목조건",
            "filter_type",
            "0:전체조회, 1:관리종목제외, 3:우선주제외, 4:우선주+관리종목제외, 5:증100제외, \
                6:증100만 보기, 7:증40만 보기, 8:증30만 보기, 9:증20만 보기, 10:우선주+관리종목+환기종목제외"
        ),
        KiwoomTransactionParameter(
            "거래량구분",
            "volume_type",
            "00000:전체조회, 00010:만주이상, 00050:5만주이상, 00100:10만주이상, 00150:15만주이상, \
                00200:20만주이상, 00300:30만주이상, 00500:50만주이상, 01000:백만주이상"
        ),
        KiwoomTransactionParameter(
            "신용조건",
            "credit_type",
            "0:전체조회, 1:신용융자A군, 2:신용융자B군, 3:신용융자C군, 4:신용융자D군, 9:신용융자전체"
        ),
        KiwoomTransactionParameter(
            "매매금구분",
            "price_type",
            "0:전체조회, 1:1천원미만, 2:1천원~2천원, 3:2천원~3천원, 4:5천원~1만원, 5:1만원이상, 8:1천원이상"
        ),
    ],
    REQUEST_OFFER_PRICE_INFO_CODE: [
        KiwoomTransactionParameter("종목코드", "stock_code", "6자리 종목코드"),
    ],
}


def get_request_offer_price_info_result_fields():
    # pylint: disable=import-outside-toplevel
    from itertools import product

    offer_types = {
        "매수": "buy",
        "매도": "sell"
    }
    line_numbers = list(range(1, 11))
    properties = {
        "잔량대비": "contrast_remaining",
        "잔량": "price",
        "호가": "remaining_volume"
    }

    fields = []

    field_info = list(product(offer_types, line_numbers, properties))
    for offer_type, line_number, property_ in field_info:
        origin_name = f"{offer_type}{line_number}선{property_}"
        changed_name = f"{offer_types[offer_type]}_{properties[property_]}_{line_number}"
        field = KiwoomTransactionResultField(origin_name, changed_name)
        fields.append(field)

    extra_fields = [
        KiwoomTransactionResultField("호가잔량기준시간", "timestamp"),
        KiwoomTransactionResultField("매도최우선잔량", "sell_top_priority_remaining_volume"),
        KiwoomTransactionResultField("매도최우선호가", "sell_top_priority_price"),
        KiwoomTransactionResultField("매수최우선호가", "buy_top_priority_price"),
        KiwoomTransactionResultField("매수최우선잔량", "buy_top_priority_remaining_volume"),
        KiwoomTransactionResultField("총매도잔량직전대비", "total_sell_remaining_volume_contrast_previous"),
        KiwoomTransactionResultField("총매도잔량", "total_sell_remaining_volume"),
        KiwoomTransactionResultField("총매수잔량", "total_buy_remaining_volume"),
        KiwoomTransactionResultField("총매수잔량직전대비", "total_buy_remaining_volume_contrast_previous"),
        KiwoomTransactionResultField(
            "시간외매도잔량대비",
            "offhour_sell_remaining_volume_contrast_previous"
        ),
        KiwoomTransactionResultField("시간외매도잔량", "offhour_sell_remaining_volume"),
        KiwoomTransactionResultField("시간외매수잔량", "offhour_buy_remaining_volume"),
        KiwoomTransactionResultField("시간외매수잔량대비", "offhour_buy_remaining_volume_contrast_previous"),
    ]

    fields.extend(extra_fields)

    return fields


REQUEST_OFFER_PRICE_INFO_RESULT_FIELDS = get_request_offer_price_info_result_fields()


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
    REQUEST_UPPER_AND_LOW_CODE: [
        KiwoomTransactionResultField("종목코드", "stock_code"),
        KiwoomTransactionResultField("종목정보", "stock_info"),
        KiwoomTransactionResultField("종목명", "stock_name"),
        KiwoomTransactionResultField("현재가", "closing"),
        KiwoomTransactionResultField("전일대비기호", "previous_contrast_symbol"),
        KiwoomTransactionResultField("전일대비", "previous_contrast_price"),
        KiwoomTransactionResultField("등락률", "fluctuation_rate"),
        KiwoomTransactionResultField("거래량", "volume"),
        KiwoomTransactionResultField("전일거래량", "previous_volume"),
        KiwoomTransactionResultField("매도잔량", "remaining_sell_volume"),
        KiwoomTransactionResultField("매도호가", "sell_offer_price"),
        KiwoomTransactionResultField("매수호가", "buy_offer_price"),
        KiwoomTransactionResultField("매수잔량", "remaining_buy_volume"),
        KiwoomTransactionResultField("횟수", "continuous_count"),
    ],
    REQUEST_OFFER_PRICE_INFO_CODE: REQUEST_OFFER_PRICE_INFO_RESULT_FIELDS,
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
