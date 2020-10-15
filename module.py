from datetime import datetime
from random import randrange
from time import sleep

from PyQt5.QAxContainer import QAxWidget

from mq import publish


def get_randomized_screen_number():
    return f"{randrange(1, 200):>04d}"


class KiwoomModule(QAxWidget):

    TIMES = 50
    WAITS = 180

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self):
        super().__init__()
        self.request_stamps = []
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        self.OnEventConnect.connect(self.on_connect)
        self.OnReceiveMsg.connect(self.on_receive_message)
        self.OnReceiveTrData.connect(self.on_receive_tr_data)

    def wait_until_request_available(self, minimum_log_second=1):
        if len(self.request_stamps) >= self.TIMES:
            delta = datetime.today() - self.request_stamps[-self.TIMES]
            waited = delta.total_seconds()
            if waited <= self.WAITS:
                waiting = self.WAITS - waited
                if waiting >= minimum_log_second:
                    print(f"{waiting:.2f} seconds wait until request available")
                sleep(waiting)
        sleep(0.2)
        self.request_stamps.append(datetime.today())

    def on_connect(self, error_code):
        publish(str(error_code), "sapi-kiwoom")
        print(error_code)

    def on_receive_message(self, screen_no, tr_id, tr_code, message):
        print(
            f"screen_no: {screen_no}, tr_id: {tr_id}, tr_code: {tr_code}, message: {message}"
        )

    def on_receive_tr_data(
        self, screen_no, tr_id, tr_code, record_name, has_next, *deprecated
    ):
        print(screen_no, tr_id, tr_code, record_name, has_next)
        publish(tr_id, "sapi-kiwoom")

    def connect(self):
        self.dynamicCall("CommConnect()")

    def set_parameter(self, key, value):
        self.dynamicCall("SetInputValue(QString, QString)", key, value)

    def set_parameters(self, parameters):
        for key, value in parameters.items():
            self.set_parameter(key, value)

    def set_day_candle(self, issue_code, to):
        params = {
            "종목코드": issue_code,
            "기준일자": f"{to:%Y%m%d}",
            "수정주가구분": "0",
        }
        self.set_parameters(params)

    def request(self, tr_id, tr_code, continuous, screen_no=None):
        if screen_no is None:
            screen_no = get_randomized_screen_number()

        return_code = self.dynamicCall(
            "CommRqData(QString, QString, int, QString)",
            tr_id,
            tr_code,
            continuous,
            screen_no,
        )
        self.wait_until_request_available()

        print(return_code)
