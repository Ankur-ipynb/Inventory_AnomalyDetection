import logging
import sqlite3
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\generate_text_data.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_and_log(msg):
    print(msg)
    logging.info(msg)

# Define constants (aligned with assumed values in ingest.py)
PRODUCTS = [f"PROD_{i}" for i in range(1, 701)]  # PROD_1 to PROD_700
LOCATIONS = ["NYC", "LA", "Chicago", "Houston", "Miami"]
CATEGORIES = ["Electronics", "Clothing", "Books", "Toys", "Food"]

def generate_inventory_data(num_records=700):
    print_and_log("Generating synthetic inventory data...")
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
    
    df = pd.DataFrame(data)
    
    # Introduce anomalies (5% of records)
    print_and_log("Introducing anomalies in quantity and price...")
    anomaly_mask = np.random.rand(num_records) < 0.05  # 5% of records
    df['is_anomaly'] = anomaly_mask
    df.loc[anomaly_mask, 'quantity'] = 1000
    df.loc[anomaly_mask, 'price'] = 5000.0
    
    return df

def main():
    try:
        # Generate synthetic data
        df = generate_inventory_data(num_records=700)
        
        # Write to SQLite
        print_and_log("Writing synthetic data to SQLite...")
        conn = sqlite3.connect('D:/inventory_project/data/inventory.db')
        df.to_sql('inventory', conn, if_exists='replace', index=False)
        conn.close()
        
        print_and_log(f"Generated and saved {len(df)} inventory records.")
    
    except Exception as e:
        print_and_log(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()