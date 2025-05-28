import sqlite3
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='D:/inventory_project/logs/introduce_anomalies.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# SQLite database path
DB_PATH = 'D:/inventory_project/data/inventory.db'

def introduce_anomalies():
    logging.info("Starting anomaly introduction process...")
    
    # Connect to the database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Fetch all records from the inventory table
        df = pd.read_sql_query("SELECT * FROM inventory", conn)
        if df.empty:
            logging.warning("No records found in inventory.db. Cannot introduce anomalies.")
            return
        
        logging.info(f"Fetched {len(df)} records from inventory.db.")
        logging.info(f"Sample data:\n{df.head().to_string()}")
        
        # Determine which records to mark as anomalies (5% of records, minimum 1 record)
        num_records = len(df)
        anomaly_count = max(1, int(num_records * 0.05))  # Ensure at least 1 record is modified
        logging.info(f"Calculated anomaly_count: {anomaly_count}")
        anomaly_indices = np.random.choice(num_records, size=anomaly_count, replace=False)
        logging.info(f"Selected anomaly indices: {anomaly_indices.tolist()}")
        logging.info(f"Introducing anomalies in {len(anomaly_indices)} records.")
        
        # Update the selected records with anomalous values
        for idx in anomaly_indices:
            # Get the record's primary key (assuming a combination of product_id, event_timestamp, store_location uniquely identifies a record)
            record = df.iloc[idx]
            product_id = record['product_id']
            event_timestamp = record['event_timestamp']
            store_location = record['store_location']
            
            # Update the record with anomalous values (quantity=1000, price=5000.0)
            cursor.execute('''
                UPDATE inventory
                SET quantity = ?, price = ?
                WHERE product_id = ? AND event_timestamp = ? AND store_location = ?
            ''', (1000, 5000.0, product_id, event_timestamp, store_location))
            
            logging.info(f"Updated record (product_id={product_id}, event_timestamp={event_timestamp}, store_location={store_location}) with anomalous values: quantity=1000, price=5000.0")
        
        # Commit the changes
        conn.commit()
        logging.info(f"Successfully introduced anomalies in {len(anomaly_indices)} records.")
        
        # Verify the updates
        updated_df = pd.read_sql_query("SELECT * FROM inventory WHERE quantity = 1000 AND price = 5000.0", conn)
        logging.info(f"Verification: Found {len(updated_df)} records with quantity=1000 and price=5000.0 after update.")
        if not updated_df.empty:
            logging.info(f"Updated records sample:\n{updated_df.to_string()}")
        
    except Exception as e:
        logging.error(f"Failed to introduce anomalies: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    introduce_anomalies()