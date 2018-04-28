import threading
import time

class myDHTTable:
    """ The distributed hash table
    """
    def __init__(self, key_range, ):
        """
        :param key_range: the range of key
        """
        self.key_range = key_range
        self.locks = [threading.Lock() for i in range(key_range)]

        self._map = {}
        self.lock_log = {}

    def put(self, key, value):
        """ If key is not in _map yet, add <key, value> to _map, otherwise update <value>
        """
        self._map[key] = value
        self.locks[key].release()

    def get(self, key):
        """
        :param key: a list of keys, if k in _map. append its value of v
        :return:
        """
        value = None
        k = key[0]

        if k in self._map:
            value = self._map[k]

        return value

    def conduct(self, message):
        status = message['status']
        op = message['op']
        key = message['key']
        value = message['value']
        from_id = message['from']
        thread_id = message['thread']

        source_info = '/'.join([str(from_id), str(thread_id)])
        if status == 'propose':
            return self.propose(key, source_info)

        elif status == 'commit':
            return self.commit(op, key, value)

        elif status == 'abort':
            return self.abort(source_info)

    def propose(self, keys, source_info):
        ret = True
        _locking = []

        for k in keys:
            if self.locks[k].acquire(False):
                _locking.append(k)
            else:
                ret = False

        self.lock_log[source_info] = _locking

        return ret

    def commit(self, op, key, value):
        if op == "get":
            return self.get(key)
        else:
            for k, v in zip(key, value):
                self.put(k, v)

            return True

    def abort(self, source_info):
        _locking = self.lock_log.pop(source_info)

        for key in _locking:
            if self.locks[key].acquire(False):
                print("ERROR! Lock is not locked" * 10)
            self.locks[key].release()

        return True








