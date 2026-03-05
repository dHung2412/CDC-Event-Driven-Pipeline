import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

DEBEZIUM_URL = os.getenv("DEBEZIUM_URL")

connector_config = {
    "name": "logistic-mysql-connector",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": "mysql",
        "database.port": "3306",
        "database.user": "root", # Debezium usually needs root to read binlogs
        "database.password": "hungbo12",
        "database.server.id": "184054",
        "topic.prefix": os.getenv("KAFKA_TOPIC_PREFIX"),
        "database.include.list": os.getenv("MYSQL_DATABASE"),
        "schema.history.internal.kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "schema.history.internal.kafka.topic": os.getenv("KAFKA_SCHEMA_HISTORY_TOPIC"),
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "true",
        "transforms.unwrap.delete.handling.mode": "rewrite"
    }
}

def register_connector():
    print(f"---------->>> Attempting to register Debezium Connector...")
    
    # Wait for Debezium API to be ready
    max_retries = 10
    for i in range(max_retries):
        try:
            response = requests.get(DEBEZIUM_URL)
            if response.status_code == 200:
                print("---------->>> Debezium REST API is ready.")
                break
        except:
            pass
        print(f"Waiting for Debezium... ({i+1}/{max_retries})")
        time.sleep(5)

    # Register
    headers = {"Content-Type": "application/json"}
    response = requests.post(DEBEZIUM_URL, data=json.dumps(connector_config), headers=headers)

    if response.status_code == 201:
        print("---------->>> Connector registered successfully!")
    elif response.status_code == 409:
        print("---------->>> Connector already exists.")
    else:
        print(f"<<<---------- Failed to register: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    register_connector()
