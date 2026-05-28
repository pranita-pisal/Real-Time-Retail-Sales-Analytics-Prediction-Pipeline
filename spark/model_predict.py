"""
Walmart ML Predictor (Pure Python - No Spark)
Consumes messages from Kafka, runs ML predictions, writes to PostgreSQL.
"""
import json
import time
import sys
import os
import joblib
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from datetime import datetime

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "walmart-sales"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5433"
POSTGRES_DB = "airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_TABLE = "sales_predictions"
MODEL_PATH = "models/sales_model.pkl"

# Global model
model = None

def load_model():
    """Loads the pre-trained scikit-learn model."""
    global model
    if model is None:
        if os.path.exists(MODEL_PATH):
            model = joblib.load(MODEL_PATH)
            print(f"  Model loaded from {MODEL_PATH}")
        else:
            print(f"  ERROR: Model not found at {MODEL_PATH}")
    return model

def get_postgres_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def create_table_if_not_exists(conn):
    """Create the sales_predictions table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
            id SERIAL PRIMARY KEY,
            store INTEGER,
            dept INTEGER,
            date TEXT,
            weekly_sales DOUBLE PRECISION,
            is_holiday BOOLEAN,
            temperature DOUBLE PRECISION,
            fuel_price DOUBLE PRECISION,
            cpi DOUBLE PRECISION,
            unemployment DOUBLE PRECISION,
            predicted_sales DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    print(f"  Table '{POSTGRES_TABLE}' is ready.")

def predict_and_write(records, conn):
    """Run ML predictions on a batch and write to PostgreSQL."""
    if not records or model is None:
        return
    
    df = pd.DataFrame(records)
    
    # Feature engineering (same as original Spark code)
    df['Date_Parsed'] = pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce')
    df['Year'] = df['Date_Parsed'].dt.year
    df['Month'] = df['Date_Parsed'].dt.month
    df['Week'] = df['Date_Parsed'].dt.isocalendar().week.astype(int)
    df['IsHoliday'] = df['IsHoliday'].apply(lambda x: 1 if x in [True, 'True', 'true', 1] else 0)
    
    feature_cols = ['Store', 'Dept', 'Week', 'Month', 'Year', 'IsHoliday', 'Temperature', 'Fuel_Price', 'CPI', 'Unemployment']
    
    # Check all features exist
    for f in feature_cols:
        if f not in df.columns:
            print(f"  Missing feature: {f}")
            return
    
    X = df[feature_cols]
    
    # Run predictions
    df['Predicted_Sales'] = model.predict(X)
    
    # Write to PostgreSQL
    cur = conn.cursor()
    insert_sql = f"""
        INSERT INTO {POSTGRES_TABLE} 
        (store, dept, date, weekly_sales, is_holiday, temperature, fuel_price, cpi, unemployment, predicted_sales)
        VALUES %s
    """
    
    values = []
    for _, row in df.iterrows():
        values.append((
            int(row['Store']),
            int(row['Dept']),
            str(row['Date']),
            float(row['Weekly_Sales']),
            bool(row['IsHoliday'] in [True, 'True', 'true', 1]),
            float(row['Temperature']),
            float(row['Fuel_Price']),
            float(row['CPI']),
            float(row['Unemployment']),
            float(row['Predicted_Sales'])
        ))
    
    execute_values(cur, insert_sql, values)
    conn.commit()
    cur.close()
    print(f"  [PostgreSQL] Wrote {len(values)} predictions. Sample prediction: ${values[0][-1]:,.2f}")

def main():
    print("=" * 50)
    print("Walmart ML Predictor (Pure Python)")
    print("=" * 50)
    
    # Load model
    load_model()
    if model is None:
        print("FATAL: Cannot proceed without a model.")
        sys.exit(1)
    
    # Connect to PostgreSQL
    print(f"Connecting to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}...")
    try:
        conn = get_postgres_connection()
        create_table_if_not_exists(conn)
        print("Connected to PostgreSQL!\n")
    except Exception as e:
        print(f"ERROR: Could not connect to PostgreSQL: {e}")
        sys.exit(1)
    
    # Connect to Kafka
    print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'ml-predictor-group-{int(time.time())}',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Connected! Listening on topic '{KAFKA_TOPIC}'...\n")
    except Exception as e:
        print(f"ERROR: Could not connect to Kafka: {e}")
        sys.exit(1)
    
    batch = []
    last_write_time = time.time()
    
    try:
        for message in consumer:
            record = message.value
            
            # Clean: skip if Weekly_Sales <= 0
            if record.get("Weekly_Sales", 0) <= 0:
                continue
            
            batch.append(record)
            
            # Process batch every 10 seconds or every 20 records
            current_time = time.time()
            if current_time - last_write_time >= 10 or len(batch) >= 20:
                try:
                    predict_and_write(batch, conn)
                except Exception as e:
                    print(f"  ERROR writing predictions: {e}")
                    # Reconnect if connection dropped
                    try:
                        conn = get_postgres_connection()
                    except:
                        pass
                batch = []
                last_write_time = current_time
                
    except KeyboardInterrupt:
        print("\nStopping ML predictor...")
        if batch:
            predict_and_write(batch, conn)
        conn.close()
        consumer.close()
        print("ML predictor stopped.")

if __name__ == "__main__":
    main()
