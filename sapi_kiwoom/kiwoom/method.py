# Method
SUBSCRIBE_REALTIME = "subscribe-realtime"
UNSUBSCRIBE_REALTIME = "unsubscribe-realtime"
GET_STOCK_NAME = "get-stock-name"
GET_STOCK_CODES = "get-stock-codes"
GET_STOCK_STATES = "get-stock-states"
REQUEST_MINUTE_CANDLE = "request-minute-candle"
REQUEST_DAY_CANDLE = "request-day-candle"
REQUEST_UPPER_AND_LOW = "request-upper-and-low"
REQUEST_OFFER_PRICE_INFO = "request-offer-price-info"
REQUEST_OFFHOUR_SINGLE_TRADE_INFO = "request-offhour-single-trade-info"
REQUEST_SHORT_TREND = "request-short-trend"


# Method type
TRANSACTION = "TRANSACTION"
REALTIME = "REALTIME"
LOOKUP = "LOOKUP"


METHOD_TYPE_MAP = {
    SUBSCRIBE_REALTIME: REALTIME,
    UNSUBSCRIBE_REALTIME: REALTIME,
    GET_STOCK_NAME: LOOKUP,
    GET_STOCK_CODES: LOOKUP,
    GET_STOCK_STATES: LOOKUP,
    REQUEST_MINUTE_CANDLE: TRANSACTION,
    REQUEST_DAY_CANDLE: TRANSACTION,
    REQUEST_UPPER_AND_LOW: TRANSACTION,
    REQUEST_OFFER_PRICE_INFO: TRANSACTION,
    REQUEST_OFFHOUR_SINGLE_TRADE_INFO: TRANSACTION,
    REQUEST_SHORT_TREND: TRANSACTION,
}


def get_method_type(method):
    if method in METHOD_TYPE_MAP:
        return METHOD_TYPE_MAP[method]
    else:
        raise KeyError(f"Method '{method}' is not exists in method map")
