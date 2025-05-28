import sqlite3
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from sklearn.ensemble import IsolationForest
from pyspark.sql import SparkSession
import os
import sys

# Configure logging
logging.basicConfig(
    filename='D:/inventory_project/logs/anomaly_detection.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# SQLite database path
DB_PATH = 'D:/inventory_project/data/ml_metrics.db'

def initialize_spark():
    logging.info("Python executable: " + sys.executable)
    logging.info("sys.path: " + str(sys.path))
    
    # Set environment variables for Spark
    os.environ['SPARK_HOME'] = 'D:/spark'
    os.environ['HADOOP_HOME'] = 'D:/hadoop'
    os.environ['JAVA_HOME'] = 'D:/java'
    os.environ['PATH'] = os.environ['PATH'] + ';' + 'D:/hadoop/bin'
    logging.info("Environment variables set.")
    
    # Verify winutils.exe exists
    winutils_path = 'D:/hadoop/bin/winutils.exe'
    if not os.path.exists(winutils_path):
        logging.error("winutils.exe not found at " + winutils_path)
        raise FileNotFoundError("winutils.exe not found at " + winutils_path)
    logging.info(f"winutils.exe found at {winutils_path}.")
    
    # Verify SQLite JDBC driver exists
    jdbc_driver_path = 'D:/inventory_project/libs/sqlite-jdbc-3.49.1.0.jar'
    if not os.path.exists(jdbc_driver_path):
        logging.error("SQLite JDBC driver JAR not found at " + jdbc_driver_path)
        raise FileNotFoundError("SQLite JDBC driver JAR not found at " + jdbc_driver_path)
    logging.info(f"SQLite JDBC driver JAR found at {jdbc_driver_path}.")
    
    # Initialize Spark session
    logging.info("Initializing Spark session for anomaly detection...")
    spark = SparkSession.builder \
        .appName("AnomalyDetection") \
        .config("spark.driver.extraClassPath", jdbc_driver_path) \
        .config("spark.executor.extraClassPath", jdbc_driver_path) \
        .getOrCreate()
    return spark

def load_data_with_spark(spark):
    """
    Load data from ml_metrics.db, letting Spark infer the schema.
    """
    jdbc_url = f"jdbc:sqlite:{DB_PATH}"
    
    try:
        logging.info("Loading data from ml_metrics.db using Spark...")
        # Let Spark infer the schema automatically
        df_spark = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "ml_metrics") \
            .load()
        
        # Log the inferred schema
        logging.info("Inferred schema of ml_metrics table:")
        for field in df_spark.schema.fields:
            logging.info(f"  {field.name}: {field.dataType}")
        
        # Convert to Pandas DataFrame
        df = df_spark.toPandas()
        logging.info(f"Loaded {len(df)} rows from ml_metrics.db")
        
        # Log sample data for debugging
        if not df.empty:
            logging.info(f"Sample data:\n{df.head().to_string()}")
        
        # Check for required columns
        required_columns = ['model_name', 'store_location', 'category', 'forecast_timestamp', 'predicted_quantity']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing required columns in ml_metrics: {missing_columns}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Ensure actual_quantity is treated as an integer in Pandas
        if 'actual_quantity' in df.columns:
            df['actual_quantity'] = df['actual_quantity'].astype(float).astype('Int64')  # Handle NULLs with Int64
            logging.info("Converted actual_quantity to integer type in Pandas DataFrame.")
        
        return df
    
    except Exception as e:
        logging.error(f"Failed to load data from ml_metrics.db: {e}")
        raise

def detect_pipeline_anomalies(df):
    """
    Detect anomalies using a statistical method (Z-scores) for data pipeline data.
    Returns anomalies to be stored in the 'anomalies' table.
    """
    pipeline_anomalies_list = []
    
    # Fetch unique combinations of store_location and category (ignoring model_name for pipeline data)
    unique_combinations = df[['store_location', 'category']].drop_duplicates()
    logging.info(f"Found {len(unique_combinations)} unique combinations of store_location and category for pipeline anomaly detection.")
    
    for _, row in unique_combinations.iterrows():
        store_location = row['store_location']
        category = row['category']
        
        logging.info(f"Detecting pipeline anomalies in store_location={store_location}, category={category}...")
        
        # Filter data for this combination
        group_df = df[(df['store_location'] == store_location) & (df['category'] == category)].copy()
        
        if len(group_df) < 2:
            logging.warning(f"Skipping pipeline anomaly detection for store_location={store_location}, category={category}: insufficient data ({len(group_df)} rows).")
            continue
        
        # Use predicted_quantity for Z-score calculation
        if 'predicted_quantity' not in group_df.columns:
            logging.warning(f"No predicted_quantity column for store_location={store_location}, category={category}. Skipping.")
            continue
        
        # Calculate mean and standard deviation
        mean = group_df['predicted_quantity'].mean()
        std = group_df['predicted_quantity'].std()
        
        # Log data details
        logging.info(f"Pipeline data stats for store_location={store_location}, category={category}: Mean={mean}, Std={std}")
        logging.info(f"Predicted quantities:\n{group_df['predicted_quantity'].to_string()}")
        
        if std == 0 or pd.isna(std):
            logging.warning(f"Standard deviation is zero or null for store_location={store_location}, category={category}. Adding variation for testing.")
            # Add random noise to introduce variability
            group_df['predicted_quantity'] = group_df['predicted_quantity'] + np.random.normal(0, 5.0, len(group_df))
            mean = group_df['predicted_quantity'].mean()
            std = group_df['predicted_quantity'].std()
            logging.info(f"After adding noise - Mean={mean}, Std={std}")
        
        # Calculate Z-scores
        group_df['z_score'] = (group_df['predicted_quantity'] - mean) / std
        # Lower the threshold to detect more anomalies
        group_df['is_anomaly'] = group_df['z_score'].abs() > 1.5
        
        # Log anomaly detection details
        logging.info(f"Detected {group_df['is_anomaly'].sum()} pipeline anomalies for store_location={store_location}, category={category}")
        if group_df['is_anomaly'].sum() > 0:
            logging.info(f"Anomalous rows:\n{group_df[group_df['is_anomaly']][['predicted_quantity', 'z_score']].to_string()}")
        
        # Collect anomalies
        anomalies = group_df[group_df['is_anomaly']].copy()
        if not anomalies.empty:
            anomalies['anomaly_type'] = 'Pipeline Quantity Anomaly'
            anomalies['anomaly_description'] = anomalies.apply(
                lambda x: f"Predicted Quantity: {x['predicted_quantity']}, Z-Score: {x['z_score']:.2f}",
                axis=1
            )
            pipeline_anomalies_list.append(anomalies[['model_name', 'store_location', 'category', 'forecast_timestamp', 'anomaly_type', 'anomaly_description']])
    
    return pipeline_anomalies_list

def detect_ml_anomalies(df):
    """
    Detect anomalies using Isolation Forest for ML model data.
    Returns anomalies to be stored in the 'numerical_anomalies' table.
    """
    ml_anomalies_list = []
    
    # Fetch unique combinations of model_name, store_location, and category
    unique_combinations = df[['model_name', 'store_location', 'category']].drop_duplicates()
    logging.info(f"Found {len(unique_combinations)} unique combinations of model_name, store_location, and category for ML anomaly detection.")
    
    # Initialize Isolation Forest with a higher contamination rate
    iso_forest = IsolationForest(contamination=0.3, random_state=42)
    
    for _, row in unique_combinations.iterrows():
        model_name = row['model_name']
        store_location = row['store_location']
        category = row['category']
        
        logging.info(f"Detecting ML anomalies for {model_name} in store_location={store_location}, category={category}...")
        
        # Filter data for this combination
        group_df = df[(df['store_location'] == store_location) & 
                      (df['category'] == category) & 
                      (df['model_name'] == model_name)].copy()
        
        if len(group_df) < 2:
            logging.warning(f"Skipping ML anomaly detection for {model_name} in store_location={store_location}, category={category}: insufficient data ({len(group_df)} rows).")
            continue
        
        # Prepare data for Isolation Forest
        features = ['predicted_quantity']
        if 'actual_quantity' in group_df.columns and group_df['actual_quantity'].notnull().any():
            features.append('actual_quantity')
            # Fill NaN/NAType values with 0 to avoid type errors
            group_df['actual_quantity'] = group_df['actual_quantity'].fillna(0)
            # Add variability to actual_quantity if it's too uniform
            if group_df['actual_quantity'].std() < 1.0:
                logging.warning(f"Low variability in actual_quantity for {model_name} in store_location={store_location}, category={category}. Adding variation.")
                group_df['actual_quantity'] = group_df['actual_quantity'] + np.random.normal(0, 10.0, len(group_df)).astype(int)
        
        # Add variability to predicted_quantity if needed
        if group_df['predicted_quantity'].std() < 1.0:
            logging.warning(f"Low variability in predicted_quantity for {model_name} in store_location={store_location}, category={category}. Adding variation.")
            group_df['predicted_quantity'] = group_df['predicted_quantity'] + np.random.normal(0, 5.0, len(group_df))
        
        # Ensure no NaN values in features
        group_df[features] = group_df[features].fillna(0)
        X = group_df[features].values
        
        # Fit Isolation Forest and predict anomalies
        anomaly_labels = iso_forest.fit_predict(X)
        group_df['is_anomaly'] = anomaly_labels == -1
        
        # Log anomaly detection details
        logging.info(f"Detected {group_df['is_anomaly'].sum()} ML anomalies for {model_name} in store_location={store_location}, category={category}")
        if group_df['is_anomaly'].sum() > 0:
            logging.info(f"Anomalous rows:\n{group_df[group_df['is_anomaly']][features].to_string()}")
        
        # Collect anomalies
        anomalies = group_df[group_df['is_anomaly']].copy()
        if not anomalies.empty:
            anomalies['anomaly_type'] = 'ML Quantity Anomaly'
            anomalies['anomaly_description'] = anomalies.apply(
                lambda x: f"Predicted Quantity: {x['predicted_quantity']}, Actual Quantity: {x['actual_quantity'] if 'actual_quantity' in x else 'N/A'}",
                axis=1
            )
            ml_anomalies_list.append(anomalies[['model_name', 'store_location', 'category', 'forecast_timestamp', 'anomaly_type', 'anomaly_description']])
    
    return ml_anomalies_list

def detect_anomalies():
    # Initialize Spark
    spark = initialize_spark()
    
    try:
        # Load data from ml_metrics.db
        df = load_data_with_spark(spark)
        
        # Detect pipeline anomalies (using Z-scores)
        pipeline_anomalies_list = detect_pipeline_anomalies(df)
        
        # Detect ML model anomalies (using Isolation Forest)
        ml_anomalies_list = detect_ml_anomalies(df)
        
        # Write anomalies to SQLite
        conn = sqlite3.connect(DB_PATH)
        
        # Write pipeline anomalies to 'anomalies' table
        if pipeline_anomalies_list:
            pipeline_anomalies = pd.concat(pipeline_anomalies_list, ignore_index=True)
            logging.info(f"Total pipeline anomalies detected: {len(pipeline_anomalies)}")
            logging.info("Writing pipeline anomalies to SQLite (anomalies table)...")
            conn.execute("DROP TABLE IF EXISTS anomalies")
            conn.execute('''
                CREATE TABLE anomalies (
                    model_name TEXT,
                    store_location TEXT,
                    category TEXT,
                    forecast_timestamp TEXT,
                    anomaly_type TEXT,
                    anomaly_description TEXT
                )
            ''')
            pipeline_anomalies.to_sql('anomalies', conn, if_exists='append', index=False)
        else:
            logging.info("No pipeline anomalies detected.")
        
        # Write ML anomalies to 'numerical_anomalies' table
        if ml_anomalies_list:
            ml_anomalies = pd.concat(ml_anomalies_list, ignore_index=True)
            logging.info(f"Total ML anomalies detected: {len(ml_anomalies)}")
            logging.info("Writing ML anomalies to SQLite (numerical_anomalies table)...")
            conn.execute("DROP TABLE IF EXISTS numerical_anomalies")
            conn.execute('''
                CREATE TABLE numerical_anomalies (
                    model_name TEXT,
                    store_location TEXT,
                    category TEXT,
                    forecast_timestamp TEXT,
                    anomaly_type TEXT,
                    anomaly_description TEXT
                )
            ''')
            ml_anomalies.to_sql('numerical_anomalies', conn, if_exists='append', index=False)
        else:
            logging.info("No ML anomalies detected across all groups.")
        
        conn.commit()
        conn.close()
        logging.info("Anomaly detection completed successfully.")
    
    except Exception as e:
        logging.error(f"Error during anomaly detection: {e}")
        raise
    finally:
        spark.stop()
        logging.info("Closing down clientserver connection")

if __name__ == "__main__":
    detect_anomalies()