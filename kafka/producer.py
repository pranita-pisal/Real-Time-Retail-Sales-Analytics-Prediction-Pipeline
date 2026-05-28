import pandas as pd
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import sys
import os

# Configuration
KAFKA_TOPIC = "walmart-sales"
KAFKA_BROKER = "localhost:9092"
CSV_FILE_PATH = "data/Walmart.csv"
DELAY_SECONDS = 0.5

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def main():
    print(f"Attempting to connect to Kafka Broker at {KAFKA_BROKER}...")
    
    try:
        # Initialize Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=json_serializer
        )
        print("Successfully connected to Kafka!")
    except NoBrokersAvailable:
        print(f"Error: No Kafka brokers available at {KAFKA_BROKER}. Is Docker running?")
        sys.exit(1)
    except KafkaError as e:
        print(f"Failed to connect to Kafka: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred while connecting: {e}")
        sys.exit(1)

    # Check if CSV exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: Dataset not found at {CSV_FILE_PATH}")
        print("Please place 'Walmart.csv' inside the 'data/' folder.")
        sys.exit(1)

    print(f"Reading data from {CSV_FILE_PATH}...")
    print(f"Publishing to topic '{KAFKA_TOPIC}' with a {DELAY_SECONDS}s delay...\n")
    
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(CSV_FILE_PATH)
        
        # Iterate over each row in the dataframe
        for index, row in df.iterrows():
            # Convert the row to a python dictionary
            message = row.to_dict()
            
            # Publish message to Kafka
            producer.send(KAFKA_TOPIC, value=message)
            
            # Print the sent message to the console
            print(f"[{index}] Sent message -> {message}")
            
            # Delay to simulate a live data stream
            time.sleep(DELAY_SECONDS)
            
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Stopping stream...")
    except Exception as e:
        print(f"\nAn error occurred during streaming: {e}")
    finally:
        # Flush ensures all buffered messages are sent before closing
        print("Flushing buffered messages...")
        producer.flush()
        producer.close()
        print("Kafka producer safely closed. Exiting.")

if __name__ == "__main__":
    main()
