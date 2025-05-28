import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import pytz

# File path for the generated data
data_file = 'D:/inventory_project/data/sample_inventory_data.csv'

# Load existing data to determine the last batch number (if any)
if os.path.exists(data_file):
    existing_df = pd.read_csv(data_file)
    if not existing_df.empty:
        # Extract batch number from product_id (e.g., P001-B1 -> 1)
        batch_numbers = existing_df['product_id'].str.extract(r'B(\d+)$')[0].astype(float)
        if batch_numbers.notna().any():
            last_batch = batch_numbers.max()
            batch_number = int(last_batch) + 1
        else:
            batch_number = 1
    else:
        batch_number = 1
else:
    batch_number = 1

# Generate 300 new records
num_records = 300
products = [f"P{str(i).zfill(3)}-B{batch_number}" for i in range(1, num_records + 1)]
quantities = np.random.randint(1, 50, size=num_records)
prices = np.round(np.random.uniform(10, 100, size=num_records), 2)
categories = np.random.choice(['Electronics', 'Clothing', 'Toys', 'Books', 'Furniture'], size=num_records)
store_ids = [f"S{str(i).zfill(2)}" for i in np.random.randint(1, 5, size=num_records)]

# Generate unique timestamps starting from the current time in IST
current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
timestamps = [(current_time + timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S') for i in range(num_records)]

# Create DataFrame (use event_timestamp instead of timestamp)
new_data = pd.DataFrame({
    'event_timestamp': timestamps,
    'product_id': products,
    'quantity': quantities,
    'price': prices,
    'store_id': store_ids,
    'category': categories
})

# Append to existing data (if any) or create new file
if os.path.exists(data_file):
    existing_df = pd.read_csv(data_file)
    updated_df = pd.concat([existing_df, new_data], ignore_index=True)
else:
    updated_df = new_data

# Save the updated data
updated_df.to_csv(data_file, index=False)

# Log the number of records
total_records = len(updated_df)
print(f"Added {len(new_data)} new records. Total records: {total_records}")