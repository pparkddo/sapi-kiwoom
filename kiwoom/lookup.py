from dataclasses import dataclass

from .method import GET_STOCK_NAME


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
