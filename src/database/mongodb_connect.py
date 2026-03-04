from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from config.database_config import MongoDBConfig

class MongoDBConnect:
    def __init__(self, config: MongoDBConfig):
        self.uri = config.uri
        self.database = config.database # String
        self.collection = config.collection

        self.client = None
        self.db_instance = None # Object

    def connect(self):
        try:
            print(f"DEBUG - MongoDB: Attempting to connect to MongoDB with URI: {self.uri}.{self.database}")
            self.client = MongoClient(self.uri)
            self.client.server_info()  # test connection
            self.db_instance = self.client[self.database]
            print(f"---------->>> Connected to MongoDB: {self.database}")
            return self.db_instance

        except ConnectionFailure as e:
            raise Exception(f"<<<---------- Failed to connect MongoDB: {e}")

    def close(self):
        if self.client:
            self.client.close()
        print("---------->>> MongoDB close")                

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()



