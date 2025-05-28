import sys
import sqlite3
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='D:/inventory_project/logs/view_ml_metrics.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Connect to SQLite database
def get_db_connection(db_path):
    conn = sqlite3.connect(db_path)
    return conn

# Check if a table exists in the database
def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?;
    """, (table_name,))
    result = cursor.fetchone()
    return result is not None

# Load ML metrics data
def load_ml_metrics_data(model_name):
    conn = get_db_connection('D:/inventory_project/data/ml_metrics.db')
    if not table_exists(conn, 'ml_metrics'):
        conn.close()
        logging.warning(f"No ml_metrics table found in ml_metrics.db for model {model_name}.")
        return pd.DataFrame()
    df = pd.read_sql_query(f"SELECT * FROM ml_metrics WHERE model_name = '{model_name}'", conn)
    conn.close()
    # Convert actual_quantity from bytes to integer if necessary
    if 'actual_quantity' in df.columns:
        df['actual_quantity'] = df['actual_quantity'].apply(
            lambda x: int.from_bytes(x, byteorder='little') if isinstance(x, bytes) else x
        )
    return df

def process_ml_metrics(model_name):
    logging.info(f"Processing ML metrics for {model_name}...")
    
    try:
        # Load the data
        df = load_ml_metrics_data(model_name)
        if df.empty:
            logging.warning(f"No data found for model {model_name} in ml_metrics.db.")
            return
        
        logging.info(f"Loaded {len(df)} rows for model {model_name}.")
        logging.info(f"Sample data:\n{df.head().to_string()}")
        
        # Basic validation: ensure required columns exist
        required_columns = ['forecast_timestamp', 'predicted_quantity']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing required columns in ml_metrics for {model_name}: {missing_columns}")
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Ensure forecast_timestamp is in datetime format
        df['forecast_timestamp'] = pd.to_datetime(df['forecast_timestamp'])
        
        # Log summary statistics
        logging.info(f"Summary statistics for predicted_quantity:\n{df['predicted_quantity'].describe().to_string()}")
        if 'actual_quantity' in df.columns and df['actual_quantity'].notnull().any():
            logging.info(f"Summary statistics for actual_quantity:\n{df['actual_quantity'].describe().to_string()}")
        
        logging.info(f"ML metrics processing for {model_name} completed successfully.")
    
    except Exception as e:
        logging.error(f"ERROR: {str(e)}")
        raise

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python view_ml_metrics.py <model_name>")
        sys.exit(1)
    
    model_name = sys.argv[1]
    logging.info(f"--- Starting View ML Metrics for {model_name} ---")
    try:
        process_ml_metrics(model_name)
    except Exception as e:
        logging.error(f"Failed to process ML metrics for {model_name}: {e}")
        sys.exit(1)