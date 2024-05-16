from contextlib import contextmanager
from queue import Queue

import pyarrow.flight as fl


class FlightConnectionPool:
    def __init__(self, host: str, port: int, pool_size: int):
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.pool = Queue(maxsize=pool_size)
        self._initialize_pool()

    def _initialize_pool(self):
        for _ in range(self.pool_size):
            client = fl.connect((self.host, self.port))
            self.pool.put(client)

    def acquire(self):
        return self.pool.get()

    def release(self, client):
        self.pool.put(client)

    @contextmanager
    def connection(self):
        client = self.acquire()
        try:
            yield client
        finally:
            self.release(client)
