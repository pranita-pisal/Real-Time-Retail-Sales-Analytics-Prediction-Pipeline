from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    'walmart_pipeline',
    default_args=default_args,
    description='Orchestration for Walmart Real-time Pipeline',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['walmart', 'streaming', 'machine_learning']
) as dag:

    # Task 1: Start the Kafka Producer
    # This will execute the python script to start streaming CSV rows to Kafka
    start_kafka_producer = BashOperator(
        task_id='start_kafka_producer',
        bash_command='python kafka/producer.py'
    )

    # Task 2: Start the Spark Aggregation Stream
    # This fires up the PySpark job to clean data and write to MongoDB
    start_spark_stream = BashOperator(
        task_id='start_spark_stream',
        bash_command='python spark/stream_processor.py'
    )

    # Task 3: Start the ML Prediction Stream
    # This launches the job that applies the Random Forest model and writes to Postgres
    start_ml_predictions = BashOperator(
        task_id='start_ml_predictions',
        bash_command='python spark/model_predict.py'
    )

    # Define the execution order (Dependencies)
    # The pipeline runs sequentially: Producer -> Aggregator -> Predictor
    start_kafka_producer >> start_spark_stream >> start_ml_predictions
