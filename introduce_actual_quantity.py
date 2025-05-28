import sqlite3
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='D:/inventory_project/logs/introduce_actual_quantity.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# SQLite database path
DB_PATH = 'D:/inventory_project/data/ml_metrics.db'

def introduce_actual_quantities():
    logging.info("Starting actual quantity introduction process...")
    
    # Connect to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Check if the ml_metrics table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='ml_metrics';
        """)
        if not cursor.fetchone():
            logging.error("ml_metrics table does not exist in ml_metrics.db. Cannot introduce actual quantities.")
            raise Exception("ml_metrics table does not exist in ml_metrics.db.")
        
        # Ensure actual_quantity column is of type INTEGER
        cursor.execute("PRAGMA table_info(ml_metrics)")
        columns = cursor.fetchall()
        actual_quantity_exists = False
        for col in columns:
            if col[1] == 'actual_quantity':
                actual_quantity_exists = True
                if col[2].upper() != 'INTEGER':
                    logging.info("actual_quantity column exists but is not INTEGER type. Altering column type...")
                    cursor.execute("ALTER TABLE ml_metrics RENAME TO ml_metrics_old")
                    cursor.execute('''
                        CREATE TABLE ml_metrics (
                            model_name TEXT,
                            store_location TEXT,
                            category TEXT,
                            forecast_timestamp TEXT,
                            predicted_quantity REAL,
                            actual_quantity INTEGER,
                            event_timestamp TEXT
                        )
                    ''')
                    cursor.execute('''
                        INSERT INTO ml_metrics
                        SELECT model_name, store_location, category, forecast_timestamp,
                               predicted_quantity, CAST(actual_quantity AS INTEGER), event_timestamp
                        FROM ml_metrics_old
                    ''')
                    cursor.execute("DROP TABLE ml_metrics_old")
                    conn.commit()
                    logging.info("Altered actual_quantity column to INTEGER type.")
                break
        
        if not actual_quantity_exists:
            logging.info("actual_quantity column does not exist. Adding it as INTEGER...")
            cursor.execute("ALTER TABLE ml_metrics ADD COLUMN actual_quantity INTEGER")
            conn.commit()
        
        # Fetch records from the ml_metrics table where actual_quantity is NULL
        df = pd.read_sql_query("SELECT * FROM ml_metrics WHERE actual_quantity IS NULL", conn)
        if df.empty:
            logging.warning("No records found with NULL actual_quantity in ml_metrics.db. No updates needed.")
            return
        
        logging.info(f"Fetched {len(df)} records with NULL actual_quantity from ml_metrics.db.")
        logging.info(f"Sample data:\n{df.head().to_string()}")
        
        # Generate random actual_quantity values (between 50 and 500)
        num_records = len(df)
        actual_quantities = np.random.randint(50, 501, size=num_records)
        logging.info(f"Generated {num_records} random actual quantities between 50 and 500.")
        
        # Define possible store_locations and categories for granularity
        store_locations = ['Store_A', 'Store_B', 'Store_C', 'Store_D']
        categories = ['Electronics', 'Clothing', 'Groceries', 'Books']
        
        # Randomly assign store_location and category to each record
        df['store_location'] = np.random.choice(store_locations, size=num_records)
        df['category'] = np.random.choice(categories, size=num_records)
        
        # Update the records with the new actual_quantity, store_location, and category values
        for idx, row in df.iterrows():
            model_name = row['model_name']
            store_location = row['store_location']
            category = row['category']
            forecast_timestamp = row['forecast_timestamp']
            new_quantity = int(actual_quantities[idx])
            
            cursor.execute('''
                UPDATE ml_metrics
                SET actual_quantity = ?, store_location = ?, category = ?
                WHERE model_name = ? AND store_location = ? AND forecast_timestamp = ?
            ''', (new_quantity, store_location, category, model_name, row['store_location'], forecast_timestamp))
            
            logging.info(f"Updated record (model_name={model_name}, store_location={store_location}, category={category}, forecast_timestamp={forecast_timestamp}) with actual_quantity={new_quantity}")
        
        # Commit the changes
        conn.commit()
        logging.info(f"Successfully introduced actual quantities in {num_records} records.")
        
        # Verify the updates
        updated_df = pd.read_sql_query("SELECT * FROM ml_metrics WHERE actual_quantity IS NOT NULL", conn)
        logging.info(f"Verification: Found {len(updated_df)} records with non-NULL actual_quantity after update.")
        if not updated_df.empty:
            logging.info(f"Updated records sample:\n{updated_df.head().to_string()}")
        
    except Exception as e:
        logging.error(f"Failed to introduce actual quantities: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    introduce_actual_quantities()