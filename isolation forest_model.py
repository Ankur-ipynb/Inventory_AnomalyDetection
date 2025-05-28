import logging
import sqlite3
import pandas as pd
import sys
import os
from pyspark.sql import SparkSession
from sklearn.ensemble import IsolationForest
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\isolationforest_model.log',
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
    print_and_log("Starting ML model: Isolation Forest...")
    print_and_log("Spark session initialized...")
    spark = SparkSession.builder \
        .appName("Isolation Forest Anomaly Detection") \
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
    
    # Convert back to Pandas for Isolation Forest
    pandas_df = spark_df.toPandas()
    
    # Prepare data for Isolation Forest
    features = ['total_quantity', 'total_stock_value', 'turnover_rate']
    pandas_df[features] = pandas_df[features].apply(pd.to_numeric, errors='coerce').fillna(0)
    
    # Train Isolation Forest model
    print_and_log("Training model...")
    model = IsolationForest(contamination=0.1, random_state=42)
    pandas_df['anomaly'] = model.fit_predict(pandas_df[features])
    normal_data = pandas_df[pandas_df['anomaly'] == 1][['total_quantity']]
    
    # Use average of normal data as "prediction"
    predicted_quantity = normal_data['total_quantity'].mean()
    
    # Generate "forecasts" for the next 7 days
    print_and_log("Generating predictions...")
    last_date = pd.to_datetime(pandas_df['event_timestamp']).max()
    forecast_dates = [last_date + timedelta(days=i) for i in range(1, 8)]
    forecast_df = pd.DataFrame({
        'forecast_timestamp': forecast_dates,
        'predicted_quantity': [predicted_quantity] * 7,
        'actual_quantity': [None] * 7,
        'model_name': ['Isolation Forest'] * 7,
        'store_location': ['All'] * 7,
        'category': ['All'] * 7,
        'event_timestamp': [str(last_date)] * 7
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