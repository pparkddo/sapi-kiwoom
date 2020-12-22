from dataclasses import dataclass

from .method import SUBSCRIBE_REALTIME, UNSUBSCRIBE_REALTIME


@dataclass
class KiwoomRealTimeParameter:
    name: str
    description: str


REAL_TIME_PARAMETERS = [
    KiwoomRealTimeParameter("stock_code", "6자리 종목코드"),
]


def validate_real_time_parameters(parameters):
    real_time_parameters = REAL_TIME_PARAMETERS

    for each in real_time_parameters:
        if each.name not in parameters:
            raise ValueError(f"Real time parameter '{each.name}'({each.description}) is missed")


def is_subscribe(method):
    return method == SUBSCRIBE_REALTIME


def is_unsubscribe(method):
    return method == UNSUBSCRIBE_REALTIME


def generate_real_time_response(stock_code, real_data_type, real_time_data):
    return {
        "stock_code": stock_code,
        "real_data_type": real_data_type,
        "real_time_data": real_time_data,
    }
