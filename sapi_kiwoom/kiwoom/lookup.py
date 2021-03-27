from dataclasses import dataclass

from .method import GET_STOCK_NAME, GET_STOCK_CODES, GET_STOCK_STATES


class KiwoomLookupError(Exception):
    pass


@dataclass
class KiwoomLookupParameter:
    name: str
    description: str


KIWOOM_LOOKUP_PARAMETER_MAP = {
    GET_STOCK_NAME: [
        KiwoomLookupParameter("stock_code", "6자리 종목코드"),
    ],
    GET_STOCK_CODES: [
        KiwoomLookupParameter(
            "market",
            "0:장내, 3:ELW, 4:뮤추얼펀드, 5:신주인수권, 6:리츠, 8:ETF, 9:하이일드펀드, 10:코스닥, 30:K-OTC, 50:코넥스(KONEX)"
        ),
    ],
    GET_STOCK_STATES: [
        KiwoomLookupParameter("stock_code", "6자리 종목코드"),
    ],
}


def get_lookup_parameters(method, parameters):
    try:
        lookup_parameters = KIWOOM_LOOKUP_PARAMETER_MAP[method]
        return [parameters[each.name] for each in lookup_parameters]
    except (KeyError, TypeError) as error:
        raise KiwoomLookupError(f"Lookup parameter '{method}', '{parameters}' is wrong") from error
