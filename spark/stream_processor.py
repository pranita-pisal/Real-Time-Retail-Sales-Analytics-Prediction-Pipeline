"""
Walmart Real-Time Stream Processor (Pure Python - No Spark)
Consumes messages from Kafka, computes aggregations, writes to MongoDB.
"""
import json
import time
import signal
import sys
from kafka import KafkaConsumer
import pymongo
import pandas as pd
from collections import defaultdict

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "walmart-sales"
MONGO_URI = "mongodb://admin:admin@localhost:27017/"
MONGO_DB = "walmart"

# Aggregation state
all_records = []

def get_mongo_client():
    return pymongo.MongoClient(MONGO_URI)

def compute_and_write_aggregations():
    """Compute aggregations and write to MongoDB."""
    if not all_records:
        return
    
    df = pd.DataFrame(all_records)
    
    client = get_mongo_client()
    db = client[MONGO_DB]
    
    # Aggregation 1: Average Weekly_Sales by Store
    agg_store = df.groupby("Store")["Weekly_Sales"].mean().reset_index()
    agg_store.columns = ["Store", "Avg_Sales_Store"]
    
    collection_store = db["sales_agg_by_store"]
    collection_store.delete_many({})  # Replace with fresh aggregation
    if len(agg_store) > 0:
        collection_store.insert_many(agg_store.to_dict("records"))
    
    # Aggregation 2: Average Weekly_Sales by IsHoliday
    agg_holiday = df.groupby("IsHoliday")["Weekly_Sales"].mean().reset_index()
    agg_holiday.columns = ["IsHoliday", "Avg_Sales_Holiday"]
    
    collection_holiday = db["sales_agg_by_holiday"]
    collection_holiday.delete_many({})  # Replace with fresh aggregation
    if len(agg_holiday) > 0:
        collection_holiday.insert_many(agg_holiday.to_dict("records"))
    
    # Also store the raw sales data for the dashboard
    collection_raw = db["sales_raw"]
    # Only insert new records (use length tracking)
    new_count = len(all_records) - getattr(compute_and_write_aggregations, '_last_count', 0)
    if new_count > 0:
        new_records = all_records[-new_count:]
        collection_raw.insert_many([dict(r) for r in new_records])
    compute_and_write_aggregations._last_count = len(all_records)
    
    client.close()
    print(f"  [MongoDB] Updated aggregations. Total records: {len(all_records)}, Stores: {len(agg_store)}")

def main():
    print("=" * 50)
    print("Walmart Stream Processor (Pure Python)")
    print("=" * 50)
    print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'stream-processor-group-{int(time.time())}',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Connected! Listening on topic '{KAFKA_TOPIC}'...\n")
    except Exception as e:
        print(f"ERROR: Could not connect to Kafka: {e}")
        sys.exit(1)
    
    batch_count = 0
    last_agg_time = time.time()
    
    try:
        for message in consumer:
            record = message.value
            
            # Clean: skip if Weekly_Sales <= 0 or missing
            if record.get("Weekly_Sales", 0) <= 0:
                continue
            
            all_records.append(record)
            batch_count += 1
            
            # Compute and write aggregations every 10 seconds or every 20 records
            current_time = time.time()
            if current_time - last_agg_time >= 10 or batch_count % 20 == 0:
                compute_and_write_aggregations()
                last_agg_time = current_time
                
    except KeyboardInterrupt:
        print("\nStopping stream processor...")
        # Final aggregation write
        compute_and_write_aggregations()
        consumer.close()
        print("Stream processor stopped.")

if __name__ == "__main__":
    main()
