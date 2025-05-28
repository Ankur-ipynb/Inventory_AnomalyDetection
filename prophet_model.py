import logging
import sqlite3
import pandas as pd
import sys
import os
from pyspark.sql import SparkSession
from prophet import Prophet
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\prophet_model.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_and_log(msg):
    print(msg)
    logging.info(msg)

# Debug: Log Python environment details
print_and_log(f"Python executable: {sys.executable}")
print_and_log(f"sys.path: {sys.path}")

# Set environment variables for Spark
os.environ['HADOOP_HOME'] = 'D:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';' + 'D:\\hadoop\\bin'
print_and_log("Environment variables set.")

# Verify winutils.exe exists
winutils_path = os.path.join(os.environ['HADOOP_HOME'], 'bin', 'winutils.exe')
if not os.path.exists(winutils_path):
    print_and_log(f"ERROR: winutils.exe not found at {winutils_path}. Spark may fail to initialize.")
    sys.exit(1)
else:
    print_and_log(f"winutils.exe found at {winutils_path}.")

# Initialize Spark session
def init_spark():
    print_and_log("Starting ML model: Prophet...")
    print_and_log("Spark session initialized...")
    spark = SparkSession.builder \
        .appName("Prophet Forecasting") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.python.worker.start.timeout", "60s") \
        .config("spark.ui.port", "4040") \
        .config("spark.ui.enabled", "true") \
        .config("spark.driver.host", "localhost") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false") \
        .config("spark.driver.memoryOverhead", "256m") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "256m") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:///D:/inventory_project/spark-events") \
        .config("spark.history.fs.logDirectory", "file:///D:/inventory_project/spark-events") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.sql.warehouse.dir", "file:///D:/inventory_project/spark-warehouse") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", "120s") \
        .config("spark.executor.heartbeatInterval", "30s") \
        .getOrCreate()
    return spark

def main():
    spark = init_spark()
    
    # Load data from SQLite using Spark
    print_and_log("Loaded data from SQLite...")
    conn = sqlite3.connect('D:\\inventory_project\\data\\inventory_metrics.db')
    df = pd.read_sql_query("SELECT * FROM metrics", conn)
    conn.close()
    
    # Convert to Spark DataFrame
    print_and_log("DataFrame created...")
    spark_df = spark.createDataFrame(df)
    
    # Convert back to Pandas for Prophet
    pandas_df = spark_df.toPandas()
    
    # Prepare data for Prophet
    pandas_df['event_timestamp'] = pd.to_datetime(pandas_df['event_timestamp'])
    prophet_df = pandas_df.groupby('event_timestamp')['total_quantity'].sum().reset_index()
    prophet_df = prophet_df.rename(columns={'event_timestamp': 'ds', 'total_quantity': 'y'})
    
    # Train Prophet model
    print_and_log("Training model...")
    model = Prophet()
    model.fit(prophet_df)
    print_and_log("Model training completed...")
    
    # Generate forecasts for the next 7 days
    print_and_log("Generating predictions...")
    future = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)
    forecast = forecast.tail(7)[['ds', 'yhat']]
    forecast = forecast.rename(columns={'ds': 'forecast_timestamp', 'yhat': 'predicted_quantity'})
    
    forecast_df = pd.DataFrame({
        'forecast_timestamp': forecast['forecast_timestamp'],
        'predicted_quantity': forecast['predicted_quantity'],
        'actual_quantity': [None] * 7,
        'model_name': ['Prophet'] * 7,
        'store_location': ['All'] * 7,
        'category': ['All'] * 7,
        'event_timestamp': [str(prophet_df['ds'].iloc[-1])] * 7
    })
    
    # Write results to ml_metrics.db
    print_and_log("Writing predictions to SQLite...")
    conn = sqlite3.connect('D:\\inventory_project\\data\\ml_metrics.db')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS ml_metrics (
            model_name TEXT,
            store_location TEXT,
            category TEXT,
            forecast_timestamp TEXT,
            predicted_quantity REAL,
            actual_quantity REAL,
            event_timestamp TEXT
        )
    ''')
    forecast_df.to_sql('ml_metrics', conn, if_exists='append', index=False)
    conn.close()
    print_and_log("ML processing completed...")
    
    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print_and_log(f"ERROR: {e}")
        sys.exit(1)