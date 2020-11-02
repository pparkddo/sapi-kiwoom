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
