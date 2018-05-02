import sys
import os
import numpy as np
import threading
import time


class myClient(threading.Thread):
    def __init__(self, from_id, thread_id, nb_operations, key_range, send_queue, recv_queue, performance_queue,
                 lock, metrics):
        threading.Thread.__init__(self)
        self.from_id = from_id
        self.thread_id = thread_id
        self.key_range = key_range
        self.send_queue = send_queue
        self.recv_queue = recv_queue
        self.nb_operations = nb_operations
        self.performance_queue = performance_queue
        self.lock = lock
        self.metrics = metrics

    def generate_operation(self, ):
        """
        Based on the prior probability, generate operator and operands
        :return: operator, key(s) and value(s)
        """

        np.random.seed()
        op = np.random.choice(['put', 'get', 'multi-put'], p=[0.2, 0.6, 0.2])
        if op == "put":
            key = np.random.randint(self.key_range, size=1).tolist()
            value = np.random.randint(10000, size=1).tolist()
        elif op == "get":
            key = np.random.randint(self.key_range, size=1).tolist()
            value = []
        else:
            key = np.random.choice(np.arange(self.key_range), replace=False, size=3).tolist()
            value = np.random.randint(10000, size=3).tolist()

        return op, key, value

    def prepare(self, op, key, value):
        """

        :param op: operation name
        :param key: key list related with op
        :param value: value list related with key
        :return:
            message_prepare: A message list.
                             For each group, it contains all the keys that store at the same node
        """
        message_prepare = []

        if op == "get":
            for k in key:
                message = dict()
                message["from"] = self.from_id
                message["to"] = k % 5
                message['thread'] = self.thread_id
                message["op"] = op
                message["key"] = key
                message["value"] = value

                message_prepare.append(message)
        else:
            partition_key = [[] for _ in range(5)]
            partition_value = [[] for _ in range(5)]
            for k, v in zip(key, value):
                node_i, node_j = k % 5, (k + 1) % 5

                partition_key[node_i].append(k)
                partition_value[node_i].append(v)

                partition_key[node_j].append(k)
                partition_value[node_j].append(v)

            for i in range(len(partition_key)):
                if len(partition_key[i]) != 0:
                    message = dict()
                    message["from"] = self.from_id
                    message["to"] = i
                    message['thread'] = self.thread_id
                    message["op"] = op
                    message["key"] = partition_key[i]
                    message["value"] = partition_value[i]

                    message_prepare.append(message)

        return message_prepare

    def collect_respond(self, nb_wait_vote, status):
        """

        :param nb_wait_vote: the number of replies to collect from servers
        :param status: the specific feedback to collect
        :return: all the expected response
        """
        respond = []

        while len(respond) < nb_wait_vote:
            message = self.recv_queue.get()
            if message['from'] == self.from_id and message['thread'] == self.thread_id \
                    and message['status'] == status:
                respond.append(message)
            else:
                self.recv_queue.put(message)

        return respond

    def conduct(self, message_prepare, action):
        """
        Conduct action with the message_prepare, to put each group of message into send_queue

        :param message_prepare: the well-parse message in a list form
        :param action: key-word to label the message, it could be 'propose', 'commit', 'abort'
        :return:
        """
        for message in message_prepare:
            message['status'] = action
            self.send_queue.put(message)

    def wait_action_done(self, nb_wait_vote, action):
        """

        :param nb_wait_vote: number of response waiting for
        :param action: the response from the server, it could be 'ack', 'commit_done', 'abort_done'
        :return: all the corresponding message in a list
        """
        response = self.collect_respond(nb_wait_vote, action)
        return response

    def run(self, ):
        for i in range(self.nb_operations):
            start_time = time.time()
            op, key, value = self.generate_operation()
            print("Epoch {} Thread-{} generate operation {} , key: {}, value: {}".\
                  format(i, self.thread_id, op, key, value))

            while True:
                message_prepare = self.prepare(op, key, value)
                nb_wait_vote = len(message_prepare)
                nb_retry = 0

                if op == 'get':
                    """Direct commit get operation"""
                    self.conduct(message_prepare, action='commit')
                    response = self.wait_action_done(nb_wait_vote, action='commit_done')

                    get_value = response[0]['reply']
                    if get_value is not None:
                        print("SUCCEED! Epoch {} Thread-{} {}, key = {}, value = {}".\
                              format(i, self.thread_id, op, key, response[0]['reply']))
                    else:
                        print("NOT FOUND! Epoch {} Thread-{} {} fail, key = {} not found".\
                              format(i, self.thread_id, op, key))

                    with self.lock:
                        self.metrics['operation_counter'] += 1
                        self.metrics['latency'] += time.time() - start_time

                    break

                else:
                    """Consistency with two-phase commit"""
                    self.conduct(message_prepare, action='propose')

                    ack_respond = self.collect_respond(nb_wait_vote, 'ack')
                    votes = [message['reply'] for message in ack_respond]
                    if votes.count(True) == len(votes):
                        """ all node vote yes """
                        self.conduct(message_prepare, 'commit')
                        self.wait_action_done(nb_wait_vote, 'commit_done')
                        print("SUCCEED! Epoch {} Thread-{} {}, key = {}, value = {}".\
                              format(i, self.thread_id, op, key, value))

                        with self.lock:
                            self.metrics['operation_counter'] += 1
                            self.metrics['latency'] += time.time() - start_time

                        break

                    else:
                        self.conduct(message_prepare, 'abort')
                        self.wait_action_done(nb_wait_vote, 'abort_done')
                        print("RETRY! Epoch {} Thread-{} {} fail, key = {}, value = {}.".\
                              format(i, self.thread_id, op, key, value))

                        nb_retry += 1
                        if nb_retry == 10:
                            break

