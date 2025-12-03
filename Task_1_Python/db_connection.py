import psycopg2
from psycopg2 import OperationalError


class DBConnection:
    """
    Class for connecting to PostgreSQL database
    """

    def __init__(self, name, user, password, host, port):
        self.name = name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self._conn = None

    def connect(self):
        """Set connection to PostgreSQL database"""
        if self._conn is not None:
            print("Connection is already active")
            return self._conn

        try:
            self._conn = psycopg2.connect(
                database=self.name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            print("Connection now is active")
            return self._conn
        except OperationalError as e:
            print(f"There is error'{e}' during DB connection")
            self._conn = None
            return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            print("Connection is closed")
            self._conn = None
        else:
            print("There isn't active connection")
            self._conn = None

    @property
    def conn(self):
        """Provides read-only access to connection object. Automatically attempts to connect to PostgreSQL database"""
        if self._conn is None:
            print("Connection is not established. Trying to connect...")
            self._conn = self.connect()
        return self._conn
