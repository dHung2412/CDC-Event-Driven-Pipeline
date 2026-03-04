import redis
from redis.exceptions import ConnectionError
from config.database_config import RedisConfig

class RedisConnect:
    def __init__(self, config: RedisConfig):
        self.host = config.host
        self.port = config.port
        self.database = config.database
        
        self.config = {
            "host": self.host,
            "port": self.port,
            "username": config.user,
            "db": self.database,
            "password": config.password if config.password else None,
            "decode_responses": True
        }
        
        self.client = None

    def connect(self):
        try:
            print(f"DEBUG - Redis: Attempting to connect to Redis: {self.host}:{self.port}")
            self.client = redis.Redis(**self.config)
            self.client.ping() # Test connection
            
            print(f"---------->>> Connected to Redis: {self.database}")
            return self.client
        except ConnectionError as e:
            raise Exception(f"<<<---------- Failed to connect Redis: {e}")

    def close(self):
        if self.client:
            self.client.close()
        print("---------->>> Redis close")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()