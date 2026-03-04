from database.mysql_connect import MySQLConnect
from database.mongodb_connect import MongoDBConnect
from database.redis_connect import RedisConnect
from config.database_config import get_database_config
from schema_manager import (create_mySQL_schema, validate_mysql_schema,
                            create_mongodb_schema, validate_mongodb_schema,
                            create_redis_schema, validated_redis_schema)
from datetime import datetime

def main(config):
    # MYSQL
    print("DEBUG: Entering MySQL block")
    with MySQLConnect(config["mysql"]) as mysql_client:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        create_mySQL_schema(connection, cursor, config["mysql"].database)
        
        cursor.execute(f"""
            INSERT INTO {config['mysql'].table_active} 
            (booking_id, shipment_type, booking_date, vehicle_no, vehicle_type, origin, destination, 
             planned_eta, actual_eta, ontime, trip_start, trip_end, distance_km, driver_name, 
             driver_mobile, customer, supplier, material) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "ACTIVE_TEST", "Regular", datetime(2020, 8, 26), "MH14GD9464", 
            "32 FT Multi-Axle 14MT - HCV", "Shive, Pune, Maharashtra", "Pondur, Kanchipuram, Tamil Nadu",
            datetime(2020, 8, 26, 3, 46), datetime(2020, 8, 28, 12, 48), 1, 
            datetime(2020, 8, 26, 16, 16), None, 1290,
            "VIRAT NILAPALLE", "9960007734", "Daimler India Commercial Vehicles Pvt Lt", 
            "Oms Logistics Pvt Ltd", "Regulator - 12v"
        ))

        connection.commit()
        print("---------->>> Inserted test data to MySQL (Active table only)")
        validate_mysql_schema(cursor)

    # MongoDB
    print("DEBUG: Entering MongoDB block")
    with MongoDBConnect(config["mongodb"]) as mongo_client:
        db = mongo_client.db_instance
        create_mongodb_schema(db, config["mongodb"].collection)
        
        db[config["mongodb"].collection].insert_one({
            "booking_id": "INIT_TEST",
            "event_type": "INITIALIZATION",
            "timestamp": datetime.now().isoformat(),
            "data": {
                "shipment_type": "Regular",
                "booking_date": datetime(2024, 3, 4),
                "distance_km": 0
            }
        })
        print(f"---------->>> Initialized collection: {config['mongodb'].collection}")
        validate_mongodb_schema(db, config["mongodb"].collection)

    # REDIS
    print("DEBUG: Entering Redis block")
    with RedisConnect(config["redis"]) as redis_client:
        client = redis_client.client
        create_redis_schema(client)
        validated_redis_schema(client)

if __name__ == "__main__":
    config = get_database_config()
    try:
        main(config)
    except Exception as e:
        print(f"ERROR: {e}")