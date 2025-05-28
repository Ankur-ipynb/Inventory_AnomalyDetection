import logging
import shutil
import sqlite3
import sys
import os
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\text_anomaly_detection.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_and_log(msg):
    print(msg)
    logging.info(msg)

# Set environment variables for Spark
os.environ['HADOOP_HOME'] = 'D:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';' + 'D:\\hadoop\\bin'
os.environ['SPARK_CLASSPATH'] = 'D:/inventory_project/libs/sqlite-jdbc-3.49.1.0.jar'
os.environ['PYSPARK_PYTHON'] = 'D:\\venv\\Scripts\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:\\venv\\Scripts\\python.exe'
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
    print_and_log("Initializing Spark session for numerical anomaly detection...")
    spark = SparkSession.builder \
        .appName("NumericalAnomalyDetection") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.python.worker.start.timeout", "120s") \
        .config("spark.ui.port", "4040") \
        .config("spark.ui.enabled", "true") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.port", "50000") \
        .config("spark.blockManager.port", "50001") \
        .config("spark.pyspark.python", "D:\\venv\\Scripts\\python.exe") \
        .config("spark.pyspark.driver.python", "D:\\venv\\Scripts\\python.exe") \
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
        .config("spark.jars", "file:///D:/inventory_project/libs/sqlite-jdbc-3.49.1.0.jar") \
        .config("spark.local.dir", "D:/inventory_project/spark-tmp") \
        .getOrCreate()
    return spark

def main():
    # Ensure custom temp directory exists
    temp_dir = "D:/inventory_project/spark-tmp"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
        print_and_log(f"Created custom Spark temp directory: {temp_dir}")

    spark = init_spark()
    
    try:
        # Load data from inventory.db
        print_and_log("Loading inventory data from inventory.db using Spark...")
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:sqlite:D:/inventory_project/data/inventory.db") \
            .option("dbtable", "inventory") \
            .option("driver", "org.sqlite.JDBC") \
            .load()
        
        row_count = df.count()
        print_and_log(f"Loaded {row_count} inventory records.")
        
        if row_count == 0:
            print_and_log("ERROR: No inventory records found in inventory.db.")
            return
        
        # Convert to Pandas for Isolation Forest
        print_and_log("Converting inventory data to Pandas for anomaly detection...")
        pandas_df = df.select("product_id", "quantity", "price", "event_timestamp", "store_location", "category").toPandas()
        features = pandas_df[['quantity', 'price']].values
        
        # Detect anomalies using Isolation Forest
        print_and_log("Detecting numerical anomalies in inventory data...")
        iso_forest = IsolationForest(contamination=0.05, random_state=42)
        labels = iso_forest.fit_predict(features)
        pandas_df['is_anomaly'] = labels == -1
        pandas_df['anomaly_method'] = 'Isolation Forest (quantity, price)'
        
        # Filter anomalies
        anomalies = pandas_df[pandas_df['is_anomaly']].copy()
        if anomalies.empty:
            print_and_log("No numerical anomalies detected.")
            return
        
        print_and_log(f"Found {len(anomalies)} numerical anomalies.")
        
        # Write anomalies to ml_metrics.db
        print_and_log("Writing numerical anomalies to SQLite...")
        conn = sqlite3.connect('D:\\inventory_project\\data\\ml_metrics.db')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS numerical_anomalies (
                product_id TEXT,
                quantity INTEGER,
                price REAL,
                event_timestamp TEXT,
                store_location TEXT,
                category TEXT,
                is_anomaly BOOLEAN,
                anomaly_method TEXT
            )
        ''')
        anomalies[['product_id', 'quantity', 'price', 'event_timestamp', 'store_location', 'category', 'is_anomaly', 'anomaly_method']] \
            .to_sql('numerical_anomalies', conn, if_exists='replace', index=False)
        conn.close()
        print_and_log("Numerical anomaly detection completed successfully.")
    
    except Exception as e:
        print_and_log(f"ERROR: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        # Manual cleanup of Spark temp directory
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
                print_and_log(f"Cleaned up Spark temp directory: {temp_dir}")
        except Exception as e:
            print_and_log(f"WARNING: Failed to clean up Spark temp directory {temp_dir}: {e}")

if __name__ == "__main__":
    main()