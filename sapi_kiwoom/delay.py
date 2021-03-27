from time import sleep
from datetime import datetime


TIMES = 50
WAITS = 180
INTERVAL = 0.2


def get_waited_seconds(request_timestamps):
    delta = datetime.today() - request_timestamps[-TIMES]
    return delta.total_seconds()


def get_seconds_to_wait(waited):
    return WAITS - waited


def log_wait(seconds_to_wait, minimum_log_second):
    if seconds_to_wait >= minimum_log_second:
        now = f"{datetime.now():%Y-%m-%d %H:%M:%S}"
        print(f"{now} Wait until request available: {seconds_to_wait:.2f} seconds")


def wait_until_request_available(request_timestamps, minimum_log_second=1):
    if len(request_timestamps) >= TIMES:
        waited_seconds = get_waited_seconds(request_timestamps)
        if waited_seconds <= WAITS:
            seconds_to_wait = get_seconds_to_wait(waited_seconds)
            log_wait(seconds_to_wait, minimum_log_second)
            sleep(seconds_to_wait)
    sleep(INTERVAL)
