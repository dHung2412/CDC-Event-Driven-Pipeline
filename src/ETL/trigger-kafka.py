import time
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer
from database.mysql_connect import MySQLConnect
from config.database_config import get_database_config

def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, timedelta):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    raise TypeError ("<<<---------- Type %s Not Serializable" % type(obj))

def get_latest_updates(mysql_client, last_timestamp, table_name):
    try:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        
        cursor.execute(f"USE {mysql_client.database}")
        
        query = f"SELECT * FROM {table_name}"
        if last_timestamp:
            query += f" WHERE updated_at > '{last_timestamp}'"
        
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        new_timestamp = last_timestamp
        if rows:
            max_ts = max(row['updated_at'] for row in rows)
            new_timestamp = max_ts.strftime('%Y-%m-%d %H:%M:%S')
            
        # Refresh snapshot to avoid stale reads (Repeatable Read isolation)
        connection.commit()
        return rows, new_timestamp
    
    except Exception as e:
        print(f"<<<---------- DEBUG - CDC Error: {e}")
        return [], last_timestamp

def determine_event_type(record):
    if record.get('trip_end'):
        return "SHIPMENT_COMPLETED"
    elif record.get('trip_start'):
        return "SHIPMENT_IN_TRANSIT"
    else:
        return "SHIPMENT_CREATED"

def main():
    config = get_database_config()
    mysql_conf = config["mysql"]
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, default=datetime_serializer).encode('utf-8')
        )
        print("---------->>> Kafka Producer Initialized")
    except Exception as e:
        print(f"<<<---------- Failed to connect to Kafka: {e}")
        return

    last_timestamp = None

    print(f"---------->>> CDC Polling Started on {mysql_conf.table_active}...")

    with MySQLConnect(mysql_conf) as mysql_client:
        while True:
            updates, new_timestamp = get_latest_updates(mysql_client, last_timestamp, mysql_conf.table_active)
            
            if updates:
                print(f"[POLL] Found {len(updates)} records | New TS: {new_timestamp}")
                for record in updates:
                    event_data = {
                        "event_type": determine_event_type(record),
                        "timestamp": datetime.now().isoformat(),
                        "data": record
                    }
                    
                    producer.send('logistic_events', value=event_data)
                    print(f"---------->>> Event Sent: {event_data['event_type']} | Booking: {record['booking_id']}")
                
                producer.flush()
                last_timestamp = new_timestamp
            
            time.sleep(10)

if __name__ == "__main__":
    main()