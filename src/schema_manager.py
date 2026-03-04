from enum import unique
from pathlib import Path
from pymysql.err import Error

#
SQL_FILE_PATH = Path(__file__).parent / "sql" / "schema.sql"

def create_mySQL_schema(connection, cursor, database):
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        connection.select_db(database)

        sql_script = SQL_FILE_PATH.read_text()
        for cmd in sql_script.split(";"):
            if cmd.strip():
                cursor.execute(cmd)
        print(f"---------->>> Executed Mysql commands")
                    
        connection.commit()
        print(f"---------->>> Created Schema in MySQL")

    except FileNotFoundError:
        raise Exception(f"<<<---------- SQL file not found at: {SQL_FILE_PATH}")
    
    except Error as e:
        connection.rollback()
        raise Exception(f"<<<---------- Failed to create Mysql Schema: {e}")

def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]

    if "Bookings_Active" not in tables or "Bookings_History" not in tables: 
        raise ValueError(f"<<<---------- Missing tables 'Bookings_Active' or 'Bookings_History'")

    # Validate data in Active table (optional test record)
    cursor.execute("SELECT booking_id FROM Bookings_Active LIMIT 1")
    if not cursor.fetchone():
        raise ValueError(f"<<<---------- No data found in 'Bookings_Active' table")

    # Validate History table by checking its structure instead of data
    cursor.execute("DESCRIBE Bookings_History")
    columns = [row[0] for row in cursor.fetchall()]
    if "booking_id" not in columns or "completed_at" not in columns:
        raise ValueError(f"<<<---------- 'Bookings_History' schema validation failed")

    print("---------->>> Validated schema in MySQL (Active with data, History structure checked)")

#
def create_mongodb_schema(database, collection_name):
    database[collection_name].drop()
    database.create_collection(collection_name)
    database[collection_name].create_index("booking_id")
    print(f"---------->>> Created Collection in MongoDB: {collection_name}")

def validate_mongodb_schema(database, collection_name):
    collections = database.list_collection_names()

    if collection_name not in collections:
        raise ValueError(f"<<<---------- Missing collection '{collection_name}' in MongoDB")
    
    print(f"---------->>> Validated Schema in MongoDB: {collection_name}")

#
def create_redis_schema(client):
    try:
        client.flushdb() # drop database
        print(f"---------->>> Redis database flushed successfully")
        
        client.set("schema_version", "gold_v1")
        client.set("last_update", "never")

        print(f"---------->>> Redis Schema Enviroment Ready")
    except Exception as e:
        raise Exception(f"<<<---------- Failed to initialize Redis Schema: {e}")

def validated_redis_schema(client):
    try:
        if not client.ping():
            raise ValueError(f"<<<---------- Redis is not responding")
        
        client.set("test_connectivity", "true")
        if client.get("test_connectivity") != "true":
            raise ValueError(f"<<<---------- Redis Write/Read check failed")

        print(f"---------->>> Validated Schema in Redis")
    except Exception as e:
        raise Exception(f"<<<---------- Failed to validate Redis Schema: {e}")