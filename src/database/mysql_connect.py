import pymysql
from pymysql.err import Error
from config.database_config import MySQLConfig

class MySQLConnect:
    def __init__(self, config: MySQLConfig):
        self.host = config.host
        self.port = config.port
        self.user = config.user
        self.password = config.password
        self.database = config.database
        self.table_active = config.table_active
        self.table_history = config.table_history
        
        self.config = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database
        }

        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            print(f"DEBUG - MySQL: Attempting to connect with config: {self.host}:{self.port}")
            self.connection = pymysql.connect(**self.config)
            self.cursor = self.connection.cursor()
            print(f"---------->>> Connected to MySQL: {self.database}")
            return self.cursor, self.connection
        
        except Error as e:
            raise Exception(f"<<<---------- Failed to connect MySQL: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print(f"---------->>> MySQL close")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()