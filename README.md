# Walmart Real-Time Sales Prediction Pipeline

## 📖 Project Overview
This project is an end-to-end, real-time data engineering and machine learning pipeline built around the Walmart sales dataset. It simulates a live stream of retail transactions, processes and cleans the data on the fly, makes live machine learning predictions, and visualizes the results on an auto-refreshing dashboard. The entire infrastructure is containerized and orchestrated for seamless deployment.

## 🏗️ Architecture Diagram

```text
                         Walmart CSV Dataset
                                  │
                                  ▼
                        Kafka Producer
                (Simulates Live Streaming Data)
                                  │
                                  ▼
                          Kafka Topic
                        (walmart-sales)
                                  │
                                  ▼
                  Spark Structured Streaming
              (Real-time Processing & Cleaning)
                                  │
                 ┌────────────────┴───────────────┐
                 ▼                                ▼
        Real-time Aggregation             ML Prediction
                 ▼                                ▼
              MongoDB                        PostgreSQL
        (sales_aggregations)            (sales_predictions)
                 │                                │
                 └────────────────┬───────────────┘
                                  ▼
                         Streamlit Dashboard
                         (Live Visualizations)
                                  ▲
                                  │
                            Apache Airflow
                  (Pipeline Scheduling & Automation)
```

## 🛠️ Tech Stack

| Component | Technology | Purpose |
| :--- | :--- | :--- |
| **Data Ingestion** | Apache Kafka | Message broker handling the live stream of sales data. |
| **Stream Processing** | Apache Spark (PySpark) | Real-time data cleaning, aggregation, and ML application. |
| **Machine Learning** | Scikit-Learn | Random Forest Regressor for sales prediction. |
| **Databases** | MongoDB & PostgreSQL | Storing real-time aggregations and ML predictions respectively. |
| **Orchestration** | Apache Airflow | Scheduling and automating the pipeline tasks. |
| **Dashboard** | Streamlit | Frontend UI for live data visualization and filtering. |
| **Containerization** | Docker Compose | Managing and networking all infrastructure services. |

## 🚀 Setup Instructions

### 1. Prerequisites & Installation
First, ensure you have Docker Desktop and Python 3.8+ installed. 
Navigate into the project directory and install the required Python packages:
```bash
pip install -r requirements.txt
```

### 2. Prepare the Data
Download the Walmart dataset and place the `Walmart.csv` file directly inside the `data/` folder.

### 3. Spin Up Infrastructure
Start the entire backend infrastructure (Kafka, Zookeeper, Spark, MongoDB, Postgres, Airflow) using Docker Compose:
```bash
PYTHONNOUSERSITE=1 docker-compose up -d
```
*(Wait a few minutes for all services, especially Airflow, to become healthy).*

### 4. Train the ML Model
Before starting the pipeline, the machine learning model must be generated.
Open `notebooks/model_training.ipynb` in Jupyter Notebook or your preferred IDE and run all cells. This will generate the `sales_model.pkl` file in the `models/` directory.

### 5. Run the Pipeline
You can orchestrate this via the Airflow UI at `http://localhost:8080`, or run them manually in separate terminal windows to see the logs flow:

**Terminal 1: Start the Live Stream**
```bash
python kafka/producer.py
```

**Terminal 2 & 3: Start Spark Processors**
```bash
python spark/stream_processor.py
python spark/model_predict.py
```

**Terminal 4: Launch the Dashboard**
```bash
streamlit run dashboard/app.py
```

## 🤖 Machine Learning Details

*   **Algorithm**: Random Forest Regressor (`sklearn.ensemble.RandomForestRegressor`)
*   **Target Variable**: `Weekly_Sales`
*   **Engineered Features**: `Store`, `Dept`, `Week`, `Month`, `Year`, `IsHoliday`, `Temperature`, `Fuel_Price`, `CPI`, `Unemployment`
*   **Evaluation Metrics**: Evaluated internally using **RMSE** (Root Mean Squared Error) and **R² Score** to determine accuracy before deployment to the live stream.

## ⚠️ Troubleshooting

**1. Kafka Error: `NoBrokersAvailable`**
*   **Cause:** The Kafka container is either down or still starting.
*   **Fix:** Run `docker ps` to ensure the `kafka` and `zookeeper` containers are running. If they are in a crash loop, ensure port `9092` and `2181` are not being used by other local applications. 

**2. Spark Error: `ClassNotFoundException: Failed to find data source: kafka`**
*   **Cause:** PySpark is missing the necessary Kafka or MongoDB connector JAR files.
*   **Fix:** Ensure you have internet access so Spark can download the dependencies defined in the `.config("spark.jars.packages", ...)` line of the scripts. 

**3. Spark JDBC Postgres Error: `Connection refused`**
*   **Cause:** Postgres container isn't ready or credentials mismatch.
*   **Fix:** Check `docker logs postgres`. Ensure you are using `admin`/`admin` as defined in your python scripts, or adjust them to match your `docker-compose.yml` if you changed them.

**4. Streamlit Dashboard is Empty**
*   **Cause:** The pipeline hasn't processed its first micro-batch.
*   **Fix:** Wait exactly 10 seconds (the Spark trigger interval) and the dashboard will auto-refresh with new data. Ensure `producer.py` is running and successfully printing logs to your console.
