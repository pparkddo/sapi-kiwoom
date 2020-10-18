from mq import MessageQueueConsumer


class KiwoomConsumer(MessageQueueConsumer):

    def __init__(self, module):
        self.module = module

    def callback(self, channel, method, properties, body):
        self.module.consume_task_request(body)


# from time import sleep
# from datetime import datetime

# TIMES = 50
# WAITS = 180

# def wait_until_request_available(self, minimum_log_second=1):
#     if len(self.request_stamps) >= self.TIMES:
#         delta = datetime.today() - self.request_stamps[-self.TIMES]
#         waited = delta.total_seconds()
#         if waited <= self.WAITS:
#             waiting = self.WAITS - waited
#             if waiting >= minimum_log_second:
#                 print(f"{waiting:.2f} seconds wait until request available")
#             sleep(waiting)
#     sleep(0.2)
#     self.request_stamps.append(datetime.today())
