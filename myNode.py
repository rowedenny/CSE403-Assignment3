import sys
import os
import threading
import socket
import time
import queue
import json
import time
import logging

from configparser import ConfigParser
from myClient import myClient
from messageSender import messageSender
from myDHTTable import myDHTTable
from RepeatTimer import RepeatTimer

MAX_DATA_SIZE = 2048

_static_lock = threading.Lock()
success_operation = 0
latency = 0.0


class myNode():
    def __init__(self, node_id,  conf_file, key_range):
        self.node_id = node_id
        self.conf_file = conf_file
        self.key_range = key_range
        self.rht_table = myDHTTable(self.key_range)

        self._lock = threading.Lock()
        self.interval = 5.0
        self.metrics = dict()
        self.metrics['operation_counter'] = 0
        self.metrics['latency'] = 0.0

        self.logger = None
        self.__init_logger()

        # read configuration file
        self.config = [None for _ in range(5)]
        self.host = None
        self.port = None
        self.read_config(conf_file)

        # build connections between nodes and keep monitor the listen_port
        self.socket_pool = [None for _ in range(5)]
        self.listen_socket = None
        self.build_connection()
        self.listen()

        self.send_queue = queue.Queue()
        self.recv_queue = queue.Queue()
        self.performance_queue = queue.Queue()

        # init sending-thread deal with send_queue
        self.send_thread = messageSender(self.send_queue, self.socket_pool)
        self.send_thread.start()

    def __init_logger(self):
        self.logger = logging.getLogger('skipGram')
        file_handler = logging.FileHandler("train.log")

        self.logger.addHandler(file_handler)
        self.logger.setLevel(logging.INFO)

    def listen(self, ):
        income_node = 0
        while True:
            connection, address = self.listen_socket.accept()
            thread = threading.Thread(target=self.message_recver, args=(connection, address))
            thread.setDaemon(True)
            thread.start()

            income_node += 1
            if income_node == len(self.socket_pool):
                break

    def message_recver(self, connection, address):
        while True:
            data = connection.recv(MAX_DATA_SIZE)
            if not data:
                break

            b = b''
            b += data
            strings = b.split(b'|')

            for message in strings:
                if not message:
                    continue

                try:
                    message = json.loads(message.decode('utf-8'))
                except:
                    continue

                if message['status'] == 'propose':
                    message['reply'] = self.rht_table.conduct(message)
                    message['status'] = 'ack'

                    destination = message['from']
                    self.socket_pool[destination].sendall(json.dumps(message).encode('utf-8') + b'|')

                elif message['status'] == 'commit':
                    message['reply'] = self.rht_table.conduct(message)
                    message['status'] = 'commit_done'

                    destination = message['from']
                    self.socket_pool[destination].sendall(json.dumps(message).encode('utf-8') + b'|')

                elif message['status'] == 'abort':
                    message['reply'] = self.rht_table.conduct(message)
                    message['status'] = 'abort_done'

                    destination = message['from']
                    self.socket_pool[destination].sendall(json.dumps(message).encode('utf-8') + b'|')

                else:
                    """ 'ack', 'commit_done', 'abort_done'
                    """
                    self.recv_queue.put(message)

    def read_config(self, conf_file):
        """
        :param conf_file: the config file for the network
                          ultimately config[i] restore the network information about all the other nodes
                          otherwise the node knows about its own configuration from the config file
        :return:
        """
        cf = ConfigParser()
        cf.read(conf_file)

        for i, section in enumerate(cf.sections()):
            node_config = {}

            for kv in cf.items(section):
                key, value = kv
                if key == "id":
                    node_config[key] = int(value)
                elif key == "host":
                    node_config[key] = value
                elif key == "port":
                    node_config[key] = int(value)
                elif key == "mod_value":
                    node_config[key] = [int(i) for i in value.split(',')]

            if self.node_id == i:
                # the listening address and port
                self.host = node_config["host"]
                self.port = node_config["port"]

            self.config[i] = node_config

    def build_connection(self, ):
        """ Making connection with nodes in the network
            the sockets in socket pool are for sending to the corresponding node
            (self.host, self.port) for listening
        """
        print("Prepared to build connection")

        # monitor the listening port
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_socket.bind((self.host, self.port))
        self.listen_socket.listen(10)

        # connect all the receiving ports
        for i, node_config in enumerate(self.config):
            host = node_config['host']
            port = node_config['port']

            while True:
                self.socket_pool[i] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    self.socket_pool[i].connect((host, port))
                    print("Connection to {} build".format((host, port)))
                    break
                except:
                    time.sleep(1)

    def monitor_thread(self, ):
        throughout = self.metrics['operation_counter'] / self.interval
        avg_latency = self.metrics['latency'] / self.metrics['operation_counter']

        with _static_lock:
            self.logger.info("Throughout = {}, average latency = {}".format(throughout, avg_latency))
            self.metrics['operation_counter'] = 0
            self.metrics['latency'] = 0.0

    def run_application(self, nb_worker=8, nb_operations=100):
        thread_list = []
        for i in range(nb_worker):
            client = myClient(self.node_id, i, nb_operations, self.key_range, self.send_queue, self.recv_queue,
                              self.performance_queue, self._lock, self.metrics)
            thread_list.append(client)
            client.start()

        RepeatTimer(self.interval, self.monitor_thread).start()

        for thread in thread_list:
            thread.join()


if __name__ == "__main__":
    node = myNode(int(sys.argv[1]), "./net_config", key_range=int(sys.argv[2]))
    node.run_application(nb_worker=8, nb_operations=100)