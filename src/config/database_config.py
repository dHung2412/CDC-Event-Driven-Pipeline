import os
from dataclasses import dataclass
from typing import Dict, Optional
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Database_config:
    def validate(self) -> None:
        for key, value in self.__dict__.items():
            if value is None:
                raise ValueError(f"_______Missing config for {key}_______")

@dataclass
class MySQLConfig(Database_config):
    host : str
    port : int
    user : str
    password : str
    database : str
    table_active : str
    table_history : str
    jar_path : Optional[str] = None

@dataclass
class MongoDBConfig(Database_config):
    uri : str
    database : str
    collection : str
    jar_path : Optional[str] = None

@dataclass
class RedisConfig(Database_config):
    host : str
    port : int
    user : str
    password : str
    database : int
    jar_path : Optional[str] = None
    key_column : str = "id"

def get_database_config()-> Dict[str,Database_config]:
    config = {
        "mysql": MySQLConfig(
            host = os.getenv("MYSQL_HOST"),
            port = int(os.getenv("MYSQL_PORT")),
            user = os.getenv("MYSQL_USER"),
            password = os.getenv("MYSQL_PASSWORD"),
            database = os.getenv("MYSQL_DATABASE"),
            table_active = os.getenv("MYSQL_TABLE_ACTIVE"),
            table_history = os.getenv("MYSQL_TABLE_HISTORY"),
            jar_path = os.getenv("MYSQL_JAR_PATH")
        ),  
        "mongodb": MongoDBConfig(
            uri = os.getenv("MONGO_URI"),
            database = os.getenv("MONGO_DB_NAME"),
            collection = os.getenv("MONGO_COLLECTION_NAME"),
            jar_path = os.getenv("MONGO_PACKAGE_PATH")
        ),
        "redis": RedisConfig(
            host = os.getenv("REDIS_HOST"),
            port = int(os.getenv("REDIS_PORT")),
            user = os.getenv("REDIS_USER"),
            password = os.getenv("REDIS_PASSWORD"),
            database = int(os.getenv("REDIS_DB")),
            jar_path = os.getenv("REDIS_JAR_PATH")
        )
    }

    for db , setting in config.items():
        setting.validate()
    return config

# test = get_database_config()
# print(test)

