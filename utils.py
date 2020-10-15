from multiprocessing import Process

from mq import consume


def start_consume_process(queue, callback):
    process = Process(target=consume, args=(queue, callback))
    process.start()
