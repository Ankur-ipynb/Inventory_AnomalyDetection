import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from pulsar import Client
import json
import time

# Configure logging
logging.basicConfig(
    filename='D:/inventory_project/logs/ingest.log',  # Match dashboard's expected log file
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# SQLite database path
DB_PATH = 'D:/inventory_project/data/inventory.db'

# Pulsar configuration
PULSAR_SERVICE_URL = 'pulsar://172.27.235.96:6650'
PULSAR_TOPIC = 'persistent://public/default/inventory-topic'

# Sample data configuration
PRODUCTS = ['P001', 'P002', 'P003', 'P004', 'P005']
LOCATIONS = ['NYC', 'LA', 'Chicago', 'Houston', 'Miami']
CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Toys', 'Food']

def generate_inventory_data(num_records=10):
    logging.info("Generating new data...")
    data = []
    for _ in range(num_records):
        record = {
            'product_id': np.random.choice(PRODUCTS),
            'quantity': np.random.randint(1, 100),
            'price': round(np.random.uniform(10, 500), 2),
            'event_timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'store_location': np.random.choice(LOCATIONS),
            'category': np.random.choice(CATEGORIES)
        }
        data.append(record)
    return pd.DataFrame(data)

def ingest_to_sqlite(df):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory (
                product_id TEXT,
                quantity INTEGER,
                price REAL,
                event_timestamp TEXT,
                store_location TEXT,
                category TEXT
            )
        ''')

        # Get existing records to avoid duplicates
        existing_records = pd.read_sql_query("SELECT * FROM inventory", conn)
        
        # If there are existing records, filter out duplicates based on key fields
        if not existing_records.empty:
            df = df.merge(
                existing_records,
                on=['product_id', 'event_timestamp', 'store_location'],
                how='left',
                indicator=True
            )
            df = df[df['_merge'] == 'left_only'].drop(columns=['_merge', 'quantity_y', 'price_y', 'category_y'])
            df = df.rename(columns={'quantity_x': 'quantity', 'price_x': 'price', 'category_x': 'category'})
        
        if df.empty:
            logging.info("No new records to insert into SQLite database")
            return
        
        # Insert new records into SQLite
        df.to_sql('inventory', conn, if_exists='append', index=False)
        conn.commit()
        logging.info(f"Inserted {len(df)} new records into SQLite database")
    
    except Exception as e:
        logging.error(f"Failed to ingest data into SQLite: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("SQLite database connection closed")

def ingest_to_pulsar(df):
    client = None
    producer = None
    for attempt in range(1, 4):
        logging.info(f"Attempting to connect to Pulsar (attempt {attempt}/3)")
        try:
            client = Client(PULSAR_SERVICE_URL)
            producer = client.create_producer(PULSAR_TOPIC)
            logging.info("Successfully connected to Pulsar")
            break
        except Exception as e:
            logging.error(f"Pulsar connection failed on attempt {attempt}: {e}")
            if attempt == 3:
                logging.error("Max retries reached. Failed to connect to Pulsar.")
                return
            time.sleep(5)

    if df.empty:
        logging.info("No new records to publish to Pulsar")
        if client:
            client.close()
        return

    # Publish new records to Pulsar
    try:
        for _, row in df.iterrows():
            message = row.to_json()
            producer.send(message.encode('utf-8'))
            logging.info(f"Published message to Pulsar: {message}")
        
        producer.flush()
        logging.info("Successfully published messages to Pulsar")
    except Exception as e:
        logging.error(f"Failed to publish messages to Pulsar: {e}")
        raise
    finally:
        if client:
            client.close()
            logging.info("Pulsar client closed")

def main():
    try:
        # Generate new inventory data
        df = generate_inventory_data(num_records=10)
        
        # Ingest to SQLite
        ingest_to_sqlite(df)
        
        # Ingest to Pulsar (only the new records that were inserted into SQLite)
        ingest_to_pulsar(df)
        
        logging.info("Ingestion completed successfully")
    except Exception as e:
        logging.error(f"Ingestion failed: {e}")
        raise

if __name__ == "__main__":
    main()