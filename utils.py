from threading import Thread
from uuid import uuid4

from mq import consume


def get_consume_thread(queue, callback):
    return Thread(target=consume, args=(queue, callback))


def get_task_id():
    return str(uuid4())


def get_task_response(task_id, result, response_time, status):
    return {
        "task_id": task_id,
        "result": result,
        "response_time": response_time,
        "status": status,
    }
