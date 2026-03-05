from kafka import KafkaConsumer
import json
import redis
import os
from datetime import datetime
from config.database_config import get_database_config
from database.mongodb_connect import MongoDBConnect
from database.redis_connect import RedisConnect
from dotenv import load_dotenv

load_dotenv()

def calculate_delay(actual_eta, planned_eta):
    try:
        if not actual_eta or not planned_eta:
            return 0

        act = datetime.fromisoformat(actual_eta.replace('Z', ''))
        pln = datetime.fromisoformat(planned_eta.replace('Z', ''))
        delay_seconds = (act - pln).total_seconds()
        return max(0, delay_seconds / 60)
    except:
        return 0

def main():
    # Debezium Topic Pattern: {topic.prefix}.{database}.{table}
    topic = "logistic_db.Logistic_Booking.Bookings_Active"
    bootstrap_servers = "localhost:9092"
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id="logistic_core_processor",
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"---------->>> Consumer Started. Monitoring: {topic}")
    except Exception as e:
        print(f"<<<---------- Failed to initialize Consumer: {e}")
        return

    # Database Connections
    db_config = get_database_config()

    mongo_conn = MongoDBConnect(db_config["mongodb"])
    db_mongo = mongo_conn.connect()
    collection_history = db_mongo["shipment_event_store"]
    
    redis_conn = RedisConnect(db_config["redis"])
    r_redis = redis_conn.connect()
    
    total_processed = 0
    
    try:
        for message in consumer:
            payload = message.value
            total_processed += 1
            
            # Debezium flattened: payload is the record itself
            booking_id = payload.get('booking_id')
            if not booking_id: continue

            event_type = "UPDATE"
            if payload.get('trip_end'):
                event_type = "SHIPMENT_COMPLETED"
            elif payload.get('trip_start'):
                event_type = "SHIPMENT_IN_TRANSIT"
            else:
                event_type = "SHIPMENT_CREATED"

            # Calculate Delay Real-Time
            delay_min = calculate_delay(payload.get('actual_eta'), payload.get('planned_eta'))
            payload['delay_minutes'] = delay_min
            payload['is_delayed'] = 1 if delay_min > 0 else 0

            payload['processed_at'] = datetime.now().isoformat()

            # 1. SINK TO MONGODB (Append History)
            collection_history.insert_one(payload)

            # 2. SINK TO REDIS (Update Current State)
            redis_key = f"shipment:{booking_id}:state"
            # Cleanup data for Redis Hash mapping
            redis_payload = {k: str(v) for k, v in payload.items() if v is not None}
            redis_payload['last_event'] = event_type
            redis_payload['last_updated'] = payload.get('processed_at')
            
            r_redis.hset(redis_key, mapping=redis_payload)
            r_redis.sadd("active_shipments", booking_id)

            if payload['is_delayed']:
                r_redis.sadd("delayed_shipments", booking_id)
            else:
                r_redis.srem("delayed_shipments", booking_id)

            print(f"[{total_processed}] {event_type} --- ID: {booking_id} --- Delay: {delay_min}m")

    except KeyboardInterrupt:
        print("\n---------->>> Consumer Stopped.")
    finally:
        consumer.close()
        r_redis.close()

if __name__ == "__main__":
    main()