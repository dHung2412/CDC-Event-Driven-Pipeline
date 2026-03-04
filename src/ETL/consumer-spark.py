from kafka import KafkaConsumer
import json
import redis
import os
from config.database_config import get_database_config
from database.mongodb_connect import MongoDBConnect
from database.redis_connect import RedisConnect

def main():
    topic = "logistic_events"
    bootstrap_servers = "localhost:9092"
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id="logistic_dashboard_group",
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"---------->>> Kafka Consumer Started. Listening for events on: {topic}")
    except Exception as e:
        print(f"<<<---------- Failed to connect to Kafka Consumer: {e}")
        return

    db_config = get_database_config()

    try: 
        mongo_conn = MongoDBConnect(db_config["mongodb"])
        db_mongo = mongo_conn.connect()
        collection_history = db_mongo["booking_events_history"]
    except Exception as e:
        print(f"<<<---------- Failed to connect to MongoDB: {e}")
        return
    
    try: 
        redis_conn = RedisConnect(db_config["redis"])
        r_redis = redis_conn.connect()
    except Exception as e:
        print(f"<<<---------- Failed to connect to Redis: {e}")
        return
    
    total_events = 0
    
    try:
        for message in consumer:
            event = message.value
            total_events += 1
            
            event_type = event.get('event_type')
            data = event.get('data', {})
            booking_id = data.get('booking_id')
            
            if not booking_id:
                continue

            # --- 1. SINK TO MONGODB (Tracing Strategy: Append History) ---
            try:
                collection_history.insert_one(event)
                mongo_status = "---> SUCCESS (History Appended)"
            except Exception as e:
                mongo_status = f"<--- FAILED: {e}"

            # --- 2. SINK TO REDIS (Real-time Strategy: Overwrite Latest) ---
            try:
                redis_key = f"realtime:booking:{booking_id}"
                redis_data = {k: str(v) for k, v in data.items() if v is not None}
                redis_data['last_event'] = event_type
                redis_data['last_updated'] = event.get('timestamp')
                
                r_redis.hset(redis_key, mapping=redis_data)
                r_redis.sadd("active_tracking_ids", booking_id)
                redis_status = "---> SUCCESS (Latest Overwritten)"
            except Exception as e:
                redis_status = f"<--- FAILED: {e}"

            print(f"\n[EVENT #{total_events}] {event_type} | Booking: {booking_id}")
            print(f" -> MongoDB: {mongo_status}")
            print(f" -> Redis:   {redis_status}")
            print("-" * 50)

    except KeyboardInterrupt:
        print("\n---------->>> Consumer Stopped by User")
    finally:
        consumer.close()
        mongo_client.close()
        r_redis.close()

if __name__ == "__main__":
    main()