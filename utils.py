from uuid import uuid4


def get_task_id():
    return str(uuid4())


def get_task_response(task_id, result, response_time, status):
    return {
        "task_id": task_id,
        "result": result,
        "response_time": response_time,
        "status": status,
    }
