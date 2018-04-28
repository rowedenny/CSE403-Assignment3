from configparser import ConfigParser
import socket
import sys
import json
import pickle
from myDHTTable import myDHTTable
from threading import Thread, Lock
from myDHTTable import myDHTTable

MAX_DATA_SIZE = 1024 * 10


class myServer:
    def __init__ ( self, id, conf_file, key_range = 10 ):
        self.id = id
        self.key_range = key_range
        self.RHT = myDHTTable(self.key_range)

        self.config = [None] * 5
        self.read_config(conf_file)

        self.listen_socket = None
        self.start_listen()

    def read_config(self, conf_file):
        self.host = None
        self.port = None

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

            if self.id == i:
                self.host = node_config["host"]
                self.port = node_config["port"]

            self.config[i] = node_config

    def start_listen(self, ):
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_socket.bind((self.host, self.port))
        self.listen_socket.listen(10)

        print("listening on ", (self.host, self.port))

    def handler(self, connection, address):
        while True:
            data = connection.recv(MAX_DATA_SIZE)

            if not data:
                break

            b = b''
            b += data
            print('-----recv-------', b.decode('utf-8'))
            d = json.loads(b.decode('utf-8'))

            op = d['op']
            key = d['key']
            value = d['value']
            locking = None

            if d['status'] == 'propose':
                status, locking = self.RHT.propose(key)
                if status:
                    connection.sendall('ack'.encode('utf-8'))
                else:
                    connection.sendall('non'.encode('utf-8'))

            elif d['status'] == 'commit':
                if d['op'] == 'get':
                    value = self.RHT.get(int(key[0]))

                    if value is not None:
                        connection.sendall(('value found {}'.format(value)).encode('utf-8'))
                    else:
                        connection.sendall('value not found'.encode('utf-8'))
                else:
                    self.RHT.commit(key, value)
                    connection.sendall('suc'.encode('utf-8'))

            elif d['status'] == 'abort':
                self.RHT.abort(locking)

    def start_server(self ):
        """ Starts a new `server_thread` for new clients
        """
        while True:
            connection, address = self.listen_socket.accept()
            print("connection from", connection)

            new_thread = Thread(target=self.handler,
                                args=(connection, address,))
            new_thread.start()



if __name__ == "__main__":
    server = myServer(int(sys.argv[1]), "./net_config", key_range = int(sys.argv[2]))
    server.start_server()

