import json
from kafka import KafkaConsumer
import sys

# Configuration
KAFKA_TOPIC = "walmart-sales"
KAFKA_BROKER = "localhost:9092"

def json_deserializer(data):
    """Deserializes the incoming bytes back into a Python dictionary."""
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        print(f"Error deserializing message: {e}")
        return data

def main():
    print(f"Attempting to connect to Kafka Broker at {KAFKA_BROKER}...")
    
    try:
        # Initialize Kafka Consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',  # Start listening to new messages immediately
            enable_auto_commit=True,
            value_deserializer=json_deserializer
        )
        
        print(f"Successfully connected! Listening to topic '{KAFKA_TOPIC}'...")
        print("Waiting for incoming messages... (Press Ctrl+C to stop)\n")
        
        # Continuously listen for new messages
        for message in consumer:
            # message.value contains the deserialized JSON payload
            print(f"Received message: {message.value}")
            
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Stopping consumer...")
    except Exception as e:
        print(f"\nAn error occurred while consuming messages: {e}")
        sys.exit(1)
    finally:
        print("Kafka consumer safely closed.")

if __name__ == "__main__":
    main()
