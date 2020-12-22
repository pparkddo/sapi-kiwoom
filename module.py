from datetime import datetime

from PyQt5.QAxContainer import QAxWidget

from messenger import MessageParsingError, get_fail_message
from mq import get_consume_thread
from delay import wait_until_request_available
from kiwoom.method import (
    get_method_type,
    REALTIME,
    LOOKUP,
    GET_STOCK_NAME,
    GET_STOCK_CODES,
    GET_STOCK_STATES,
)
from kiwoom.lookup import get_lookup_parameters, KiwoomLookupError
from kiwoom.transaction import (
    KiwoomTransactionRequest,
    get_transaction_response,
    is_last_transaction_data,
    KIWOOM_CONTINUE_REQUEST,
    REQUEST_SUCCEED,
    get_randomized_screen_number,
)
from kiwoom.task import validate_task_parameters, KiwoomTask, REQUESTED, FAILED
from kiwoom.rt import (
    validate_real_time_parameters,
    is_subscribe,
    is_unsubscribe,
    generate_real_time_response,
)


# Kiwoom Connection Status
CONNECTION_SUCCEED = 0


class KiwoomModule(QAxWidget):

    def __init__(self, messenger):
        super().__init__()

        self.tasks = {}
        self.screen_numbers = {}  # {stock_code: screen_number,}
        self.listeners = {}  # {stock_code: [task_id,],}

        self.messenger = messenger
        self.consumer = get_consume_thread("tasks", self.callback)

        self.request_timestamps = []

        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

        self.OnEventConnect.connect(self.on_connect)
        self.OnReceiveMsg.connect(self.on_receive_message)
        self.OnReceiveTrData.connect(self.on_receive_tr_data)
        self.OnReceiveRealData.connect(self.on_receive_real_data)

    def add_listener(self, stock_code, task_id):
        existing_listeners = self.listeners.get(stock_code, [])
        listeners = [*existing_listeners, task_id]
        self.listeners.update({stock_code: listeners})

    def add_screen_number(self, stock_code, screen_number):
        if stock_code not in self.screen_numbers:
            self.screen_numbers.update({stock_code: screen_number})

    def remove_listener(self, stock_code, task_id):
        existing_listeners = self.listeners[stock_code]
        listeners = list(filter(lambda each: each != task_id, existing_listeners))
        if not listeners:
            self.listeners.pop(stock_code)
            return
        self.listeners.update({stock_code: listeners})

    def remove_screen_number(self, stock_code):
        existing_listeners = self.listeners.get(stock_code, [])
        if existing_listeners:
            return
        self.screen_numbers.pop(stock_code)

    def get_screen_number(self, stock_code):
        if stock_code not in self.screen_numbers:
            raise ValueError(f"{stock_code} is not subscribed")
        return self.screen_numbers[stock_code]

    def has_subscribed(self, stock_code, task_id):
        listeners = self.listeners.get(stock_code, [])
        return task_id in listeners

    def start_consuming(self):
        return self.consumer.start()

    def callback(self, channel, method, properties, body):
        try:
            message = self.messenger.parse_message(channel, method, properties, body)
            self.handle_task_request(message)
        except MessageParsingError as error:
            task_response = get_fail_message("unknown", str(error))
            delivery_tag = self.messenger.generate_delivery_tag(method)
            reply_queue = self.messenger.generate_reply_queue(properties)
            self.messenger.send(task_response, reply_queue, channel)
            self.messenger.acknowledge(channel, delivery_tag)
        except (KeyError, ValueError, KiwoomLookupError) as error:
            self.messenger.send_fail_message(message.task_id, str(error))
        except Exception as error:  # pylint: disable=broad-except
            print(f"Unhandled excpetion: {error}")
            self.messenger.send_fail_message(message.task_id, "Unhandled excpetion occurred")

    def handle_task_request(self, message):
        task_id = message.task_id
        method = message.method
        parameters = message.parameters

        method_type = get_method_type(method)

        if method_type == REALTIME:
            validate_real_time_parameters(parameters)

            stock_code = parameters["stock_code"]

            if is_subscribe(method):
                if not self.has_subscribed(stock_code, task_id):
                    screen_number = get_randomized_screen_number()
                    self.subscribe_real_time_data(screen_number, stock_code, "10", "0")
                    self.add_listener(stock_code, task_id)
                    self.add_screen_number(stock_code, screen_number)
                self.messenger.send_success_message(
                    task_id,
                    f"{task_id} subscribes {stock_code} successfully",
                    pop_reply_queue=False
                )
            elif is_unsubscribe(method):
                if not self.has_subscribed(stock_code, task_id):
                    self.messenger.send_fail_message(
                        task_id,
                        f"{task_id} did not subscribed {stock_code} yet"
                    )
                    return
                screen_number = self.get_screen_number(stock_code)
                self.unsubscribe_real_time_date(screen_number, stock_code)
                self.remove_listener(stock_code, task_id)
                self.remove_screen_number(stock_code)
                self.messenger.send_success_message(
                    task_id,
                    f"{task_id} unsubscribes {stock_code} successfully"
                )
            else:
                self.send_fail_message(task_id, f"Method '{method}' is not avaliable")
                return

        elif method_type == LOOKUP:
            lookup_result = self.get_lookup_result(method, parameters)
            self.messenger.send_success_message(task_id, lookup_result)
        else:
            validate_task_parameters(method, parameters)
            task = KiwoomTask(message)

            transaction_request = KiwoomTransactionRequest(
                task_id,
                method,
                parameters
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
            print("Connection Success")
            self.start_consuming()
        else:
            print("Connection Failed")

    def on_receive_message(self, screen_number, tr_id, tr_code, message):
        print(
            f"screen_number: {screen_number}, \
              tr_id: {tr_id}, \
              tr_code: {tr_code}, \
              message: {message}"
        )

    def on_receive_tr_data(
            self,
            screen_number,
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
            self.messenger.send_success_message(task_id, current_task.filtered_responses)
        else:
            current_task.transaction_request.continuous = KIWOOM_CONTINUE_REQUEST
            self.request(current_task.transaction_request)

    def on_receive_real_data(self, stock_code, real_data_type, real_time_data):
        # pylint: disable=unused-argument
        listeners = self.listeners.get(stock_code)
        if not listeners:
            return

        real_time_response = generate_real_time_response(stock_code, real_data_type, real_time_data)
        for listener in listeners:
            self.messenger.send_success_message(listener, real_time_response, False, False)

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
            transaction_request.screen_number,
        )
        current_task = self.get_task(transaction_request.transaction_id)
        if return_code == REQUEST_SUCCEED:
            current_task.status = REQUESTED
        else:
            current_task.status = FAILED
            self.messenger.send_fail_message(transaction_request.transaction_id, [])

    def get_transaction_data(self, transcation_code, task_id):
        return self.dynamicCall("GetCommDataEx(QString, QString)", transcation_code, task_id)

    def subscribe_real_time_data(self, screen_number, stock_code, fids, real_data_type):
        self.dynamicCall(
            "SetRealReg(QString, QString, QString, QString)",
            screen_number,
            stock_code,
            fids,
            real_data_type
        )

    def unsubscribe_real_time_date(self, screen_number, stock_code):
        self.dynamicCall("SetRealRemove(QString, QString)", screen_number, stock_code)

    def get_lookup_result(self, method, parameters):
        lookup_paramters = get_lookup_parameters(method, parameters)
        if method == GET_STOCK_NAME:
            return self.get_stock_name(lookup_paramters)
        if method == GET_STOCK_CODES:
            return self.get_stock_codes(lookup_paramters)
        if method == GET_STOCK_STATES:
            return self.get_stock_states(lookup_paramters)

    def get_stock_name(self, stock_code):
        return self.dynamicCall("GetMasterCodeName(QString)", stock_code)

    def get_stock_codes(self, market):
        return self.dynamicCall("GetCodeListByMarket(QString)", market)

    def get_stock_states(self, stock_code):
        return self.dynamicCall("GetMasterStockState(QString)", stock_code)
