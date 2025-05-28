import logging
import shutil
import sys
import os
import time
from pyspark.sql import SparkSession
import pandas as pd
import sqlite3
import re

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\query.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_and_log(msg):
    print(msg)
    logging.info(msg)

# Set environment variables for Spark
os.environ['HADOOP_HOME'] = 'D:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';' + 'D:\\hadoop\\bin'
os.environ['SPARK_CLASSPATH'] = 'D:\\inventory_project\\libs\\sqlite-jdbc-3.49.1.0.jar'
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

# Verify hadoop.dll exists to suppress NativeCodeLoader warning
hadoop_dll_path = os.path.join(os.environ['HADOOP_HOME'], 'bin', 'hadoop.dll')
if not os.path.exists(hadoop_dll_path):
    print_and_log(f"WARNING: hadoop.dll not found at {hadoop_dll_path}. You may see NativeCodeLoader warnings.")

# Initialize Spark session
def init_spark():
    print_and_log("Initializing Spark session for dashboard query...")
    # Use a local Windows path with backslashes
    temp_dir = "D:\\inventory_project\\spark-tmp"
    spark = SparkSession.builder \
        .appName("DashboardQuery") \
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
        .config("spark.local.dir", temp_dir) \
        .config("spark.log.level", "ERROR") \
        .getOrCreate()
    # Set log level to reduce warnings
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def generate_notification(row):
    """Generate a notification message for the dashboard."""
    # Extract predicted_quantity and actual_quantity from anomaly_description
    predicted_quantity = None
    actual_quantity = None
    description = row['anomaly_description']
    pred_match = re.search(r"Predicted Quantity: ([\d.]+)", description)
    actual_match = re.search(r"Actual Quantity: ([\d.]+|N/A)", description)
    if pred_match:
        predicted_quantity = float(pred_match.group(1))
    if actual_match:
        actual_qty_str = actual_match.group(1)
        actual_quantity = float(actual_qty_str) if actual_qty_str != 'N/A' else 'N/A'

    return (f"⚠️ Anomaly Detected in Inventory!\n"
            f"Model: {row['model_name']}\n"
            f"Predicted Quantity: {predicted_quantity if predicted_quantity is not None else 'N/A'}\n"
            f"Actual Quantity: {actual_quantity if actual_quantity is not None else 'N/A'}\n"
            f"Store Location: {row['store_location']}\n"
            f"Timestamp: {row['forecast_timestamp']}\n"
            f"Category: {row['category']}\n"
            f"Anomaly Type: {row['anomaly_type']}\n"
            "This unusual quantity may indicate an inventory issue. Investigate immediately!")

def handle_user_query(query):
    """Handle user queries with a simple keyword-based response system."""
    query = query.lower()
    if 'inventory issue' in query:
        return "The anomaly suggests a potential inventory issue. Check stock levels and recent transactions for the affected store location."
    elif 'price' in query:
        return "The anomaly indicates an unusual price. Verify pricing data and check for errors or promotions."
    elif 'quantity' in query:
        return "The anomaly indicates an unusual quantity. Investigate stock levels and recent sales for the product."
    elif 'next steps' in query:
        return "Recommended next steps: 1) Investigate the affected store location or product. 2) Verify data accuracy. 3) Adjust inventory or pricing as needed."
    else:
        return "I'm not sure how to respond to that query. Try asking about 'inventory issue', 'price', 'quantity', or 'next steps'."

def main():
    # Ensure custom temp directory exists
    temp_dir = "D:\\inventory_project\\spark-tmp"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
        print_and_log(f"Created custom Spark temp directory: {temp_dir}")

    spark = init_spark()
    
    try:
        # Load numerical anomalies from ml_metrics.db
        print_and_log("Loading numerical anomalies from ml_metrics.db using Spark...")
        numerical_anomalies = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:sqlite:D:/inventory_project/data/ml_metrics.db") \
            .option("dbtable", "numerical_anomalies") \
            .option("driver", "org.sqlite.JDBC") \
            .load()
        
        # Convert to Pandas for notification generation
        numerical_anomalies_pd = numerical_anomalies.toPandas()
        
        # Generate notifications for numerical anomalies
        notifications = []
        if not numerical_anomalies_pd.empty:
            print_and_log(f"Found {len(numerical_anomalies_pd)} numerical anomalies.")
            for _, row in numerical_anomalies_pd.iterrows():
                notification = generate_notification(row)
                notifications.append({
                    'type': 'numerical',
                    'notification': notification,
                    'store_location': row['store_location'],
                    'event_timestamp': row['forecast_timestamp'],  # Use forecast_timestamp as event_timestamp
                    'product_id': 'N/A',  # Placeholder since product_id is not available
                    'category': row['category']  # Add category
                })
        else:
            print_and_log("No numerical anomalies found to generate notifications.")
            return
        
        # Write notifications to ml_metrics.db
        print_and_log("Writing notifications to SQLite for dashboard...")
        notifications_df = pd.DataFrame(notifications)
        conn = sqlite3.connect('D:\\inventory_project\\data\\ml_metrics.db')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS dashboard_notifications (
                type TEXT,
                notification TEXT,
                store_location TEXT,
                event_timestamp TEXT,
                product_id TEXT,
                category TEXT  -- Added category column
            )
        ''')
        notifications_df.to_sql('dashboard_notifications', conn, if_exists='replace', index=False)
        conn.close()
        print_and_log("Dashboard notifications generated successfully.")
    
    except Exception as e:
        print_and_log(f"ERROR: {e}")
        raise  # Re-raise the exception to ensure the subprocess returns a non-zero exit code
    finally:
        # Stop Spark session before cleanup
        spark.stop()
        # Manual cleanup of Spark temp directory with retry logic
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir, ignore_errors=False)
                    print_and_log(f"Cleaned up Spark temp directory: {temp_dir}")
                    break
                else:
                    print_and_log(f"Spark temp directory {temp_dir} does not exist, no cleanup needed.")
                    break
            except Exception as e:
                if attempt < max_attempts - 1:
                    print_and_log(f"Retrying cleanup of Spark temp directory {temp_dir} (Attempt {attempt + 1}/{max_attempts})...")
                    time.sleep(1)  # Wait before retrying
                else:
                    print_and_log(f"WARNING: Failed to clean up Spark temp directory {temp_dir} after {max_attempts} attempts: {e}")

if __name__ == "__main__":
    main()