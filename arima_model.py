import logging
import sqlite3
import pandas as pd
import sys
import os
from pyspark.sql import SparkSession
from statsmodels.tsa.arima.model import ARIMA
import numpy as np
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\arima_model.log',
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
    print_and_log("Starting ML model: ARIMA...")
    print_and_log("Spark session initialized...")
    spark = SparkSession.builder \
        .appName("ARIMAForecasting") \
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
    
    # Debug: Log the loaded data
    print_and_log(f"Loaded {len(df)} rows from inventory_metrics.db")
    if df.empty:
        print_and_log("ERROR: No data found in inventory_metrics.db. Cannot proceed with ARIMA modeling.")
        spark.stop()
        sys.exit(1)
    
    # Convert to Spark DataFrame
    print_and_log("DataFrame created...")
    spark_df = spark.createDataFrame(df)
    
    # Convert back to Pandas for ARIMA
    pandas_df = spark_df.toPandas()
    
    # Debug: Log the Pandas DataFrame
    print_and_log(f"Pandas DataFrame has {len(pandas_df)} rows")
    
    # Prepare data for ARIMA: Group by store_location and event_timestamp
    pandas_df['event_timestamp'] = pd.to_datetime(pandas_df['event_timestamp'])
    pandas_df = pandas_df.sort_values('event_timestamp')
    
    # Debug: Log unique timestamps
    unique_timestamps = pandas_df['event_timestamp'].nunique()
    print_and_log(f"Number of unique timestamps: {unique_timestamps}")
    
    # Check if we have enough timestamps overall
    if unique_timestamps < 3:
        print_and_log(f"WARNING: Only {unique_timestamps} unique timestamps found. Attempting to model per store_location.")
    
    # Group by store_location to create separate time series
    grouped = pandas_df.groupby('store_location')
    forecast_dfs = []
    
    for store_location, group in grouped:
        print_and_log(f"Processing store_location: {store_location}")
        time_series = group.groupby('event_timestamp')['total_quantity'].sum()
        
        # Debug: Log the time series data for this store_location
        print_and_log(f"Time series for {store_location} has {len(time_series)} data points")
        print_and_log(f"Time series data for {store_location}:\n{time_series}")
        
        # Check if time_series has enough data points for ARIMA
        if len(time_series) < 3:
            print_and_log(f"WARNING: Insufficient data points ({len(time_series)}) for ARIMA modeling in {store_location}. Using moving average as fallback.")
            # Fallback: Use a simple moving average for the last timestamp
            avg_quantity = time_series.mean()
            forecast_values = [avg_quantity]
            actual_quantity = time_series.iloc[-1]  # Use the last actual value
            forecast_dates = [time_series.index[-1]]
        else:
            # Split the time series: Use all but the last point for training
            train_series = time_series.iloc[:-1]
            actual_quantity = time_series.iloc[-1]  # The last point is the actual value
            forecast_dates = [time_series.index[-1]]
            
            # Train ARIMA model on the training data
            print_and_log(f"Training ARIMA model for {store_location}...")
            order = (1, 1, 1) if len(train_series) > 5 else (1, 0, 1)
            print_and_log(f"Using ARIMA order: {order}")
            try:
                model = ARIMA(train_series, order=order)
                model_fit = model.fit()
                print_and_log(f"Model training completed for {store_location}...")
                
                # Generate forecast for the last timestamp (1 step ahead)
                print_and_log(f"Generating predictions for {store_location}...")
                forecast = model_fit.forecast(steps=1)
                
                # Debug: Log the forecast output
                print_and_log(f"Forecast output type for {store_location}: {type(forecast)}")
                print_and_log(f"Forecast output for {store_location}:\n{forecast}")
                
                # Ensure forecast is a 1-dimensional array
                if isinstance(forecast, (pd.Series, np.ndarray)):
                    forecast_values = forecast.tolist()
                else:
                    forecast_values = [forecast]
                    print_and_log(f"WARNING: Forecast output for {store_location} is a scalar. Using as is.")
            except Exception as e:
                print_and_log(f"ERROR: ARIMA modeling failed for {store_location}: {str(e)}. Using moving average as fallback.")
                avg_quantity = train_series.mean()
                forecast_values = [avg_quantity]
        
        # Create forecast DataFrame for this store_location
        forecast_df = pd.DataFrame({
            'forecast_timestamp': forecast_dates,
            'predicted_quantity': forecast_values,
            'actual_quantity': [actual_quantity],
            'model_name': ['ARIMA'],
            'store_location': [store_location],
            'category': ['All'],
            'event_timestamp': [str(train_series.index[-1]) if len(time_series) >= 3 else str(time_series.index[-2])]
        })
        forecast_dfs.append(forecast_df)
    
    # Check if any forecasts were generated
    if not forecast_dfs:
        print_and_log("ERROR: No valid time series found for any store_location. Cannot proceed.")
        spark.stop()
        sys.exit(1)
    
    # Combine forecasts from all store_locations
    final_forecast_df = pd.concat(forecast_dfs, ignore_index=True)
    
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
    # Clear existing ARIMA data to avoid duplicates
    conn.execute("DELETE FROM ml_metrics WHERE model_name = 'ARIMA'")
    final_forecast_df.to_sql('ml_metrics', conn, if_exists='append', index=False)
    conn.close()
    print_and_log("ML processing completed...")
    
    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print_and_log(f"ERROR: {e}")
        sys.exit(1)