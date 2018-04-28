import sys
import os
import numpy as np
import threading
import time
import json


class messageSender(threading.Thread):
    def __init__(self, send_queue, socket_pool):
        threading.Thread.__init__(self)
        self.send_queue = send_queue
        self.socket_pool = socket_pool

    def run(self, ):
        while True:
            message = self.send_queue.get()
            self.send(message)
            self.send_queue.task_done()

    def send(self, message):
        destination = message['to']
        self.socket_pool[destination].sendall(json.dumps(message).encode('utf-8') + b'|')

