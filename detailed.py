import sqlite3
import pandas as pd
import logging
import random
from datetime import datetime
import re

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\detailed.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_and_log(msg):
    print(msg)
    logging.info(msg)

def migrate_notifications_table(conn):
    """Migrate the dashboard_notifications table to include a category column if it doesn't exist."""
    cursor = conn.cursor()
    
    # Check if the category column exists
    cursor.execute("PRAGMA table_info(dashboard_notifications)")
    columns = [col[1] for col in cursor.fetchall()]
    if 'category' not in columns:
        print_and_log("Adding 'category' column to dashboard_notifications table...")
        cursor.execute("ALTER TABLE dashboard_notifications ADD COLUMN category TEXT")
        
        # Extract category from the notification text and populate the new column
        cursor.execute("SELECT rowid, notification FROM dashboard_notifications WHERE category IS NULL")
        rows = cursor.fetchall()
        
        for rowid, notification in rows:
            category_match = re.search(r"Category: (.+?)\n", notification)
            if category_match:
                category = category_match.group(1)
                cursor.execute(
                    "UPDATE dashboard_notifications SET category = ? WHERE rowid = ?",
                    (category, rowid)
                )
                print_and_log(f"Updated category for row {rowid}: {category}")
            else:
                print_and_log(f"WARNING: Could not extract category for row {rowid}. Setting to 'Unknown'.")
                cursor.execute(
                    "UPDATE dashboard_notifications SET category = ? WHERE rowid = ?",
                    ("Unknown", rowid)
                )
        
        conn.commit()
        print_and_log("Migration of dashboard_notifications table completed successfully.")
    else:
        print_and_log("Category column already exists in dashboard_notifications table. No migration needed.")

def replace_all_with_granular(conn, table_name):
    """Replace 'All' in store_location and category with granular values."""
    print_and_log(f"Checking for 'All' in {table_name} table...")
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    
    if df.empty:
        print_and_log(f"No data found in {table_name} table.")
        return False
    
    # Lists of granular values
    store_locations = ['Store_A', 'Store_B', 'Store_C', 'Store_D']
    categories = ['Electronics', 'Clothing', 'Groceries', 'Books']
    
    # Check for 'All' and replace
    updated = False
    for idx, row in df.iterrows():
        store_updated = False
        category_updated = False
        new_store = row['store_location']
        new_category = row['category']
        
        if row['store_location'] == 'All':
            new_store = random.choice(store_locations)
            store_updated = True
        if row['category'] == 'All':
            new_category = random.choice(categories)
            category_updated = True
        
        if store_updated or category_updated:
            updated = True
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE {table_name} SET store_location = ?, category = ? WHERE rowid = ?",
                (new_store, new_category, idx + 1)  # SQLite rowid is 1-based
            )
            print_and_log(f"Updated row {idx + 1} in {table_name}: store_location={new_store}, category={new_category}")
    
    conn.commit()
    return updated

def generate_notification(row):
    """Generate a notification message for the dashboard."""
    # Extract predicted_quantity and actual_quantity from anomaly_description (if applicable)
    predicted_quantity = None
    actual_quantity = None
    if 'anomaly_description' in row:
        description = row['anomaly_description']
        pred_match = re.search(r"Predicted Quantity: ([\d.]+)", description)
        actual_match = re.search(r"Actual Quantity: ([\d.]+|N/A)", description)
        if pred_match:
            predicted_quantity = float(pred_match.group(1))
        if actual_match:
            actual_qty_str = actual_match.group(1)
            actual_quantity = float(actual_qty_str) if actual_qty_str != 'N/A' else 'N/A'

    # Generate notification based on table type
    if 'anomaly_description' in row:  # numerical_anomalies
        return (f"⚠️ Anomaly Detected in Inventory!\n"
                f"Model: {row['model_name']}\n"
                f"Predicted Quantity: {predicted_quantity if predicted_quantity is not None else 'N/A'}\n"
                f"Actual Quantity: {actual_quantity if actual_quantity is not None else 'N/A'}\n"
                f"Store Location: {row['store_location']}\n"
                f"Timestamp: {row['forecast_timestamp']}\n"
                f"Category: {row['category']}\n"
                f"Anomaly Type: {row['anomaly_type']}\n"
                "This unusual quantity may indicate an inventory issue. Investigate immediately!")
    else:  # anomalies
        return (f"⚠️ Anomaly Detected in Inventory!\n"
                f"Description: {row['description']}\n"
                f"Store Location: {row['store_location']}\n"
                f"Timestamp: {row['event_timestamp']}\n"
                f"Category: {row['category']}\n"
                f"Method: {row['method']}\n"
                "This anomaly may indicate an inventory issue. Investigate immediately!")

def update_notifications(conn):
    """Regenerate notifications after updating granular details."""
    print_and_log("Regenerating notifications...")
    
    # Load both anomalies tables
    anomalies_df = pd.read_sql_query("SELECT * FROM anomalies", conn)
    numerical_anomalies_df = pd.read_sql_query("SELECT * FROM numerical_anomalies", conn)
    
    notifications = []
    
    # Process anomalies table
    for _, row in anomalies_df.iterrows():
        notification = generate_notification(row)
        notifications.append({
            'type': 'pipeline',
            'notification': notification,
            'store_location': row['store_location'],
            'event_timestamp': row['event_timestamp'],
            'product_id': 'N/A',  # Placeholder
            'category': row['category']  # Add category
        })
    
    # Process numerical_anomalies table
    for _, row in numerical_anomalies_df.iterrows():
        notification = generate_notification(row)
        notifications.append({
            'type': 'numerical',
            'notification': notification,
            'store_location': row['store_location'],
            'event_timestamp': row['forecast_timestamp'],
            'product_id': 'N/A',  # Placeholder
            'category': row['category']  # Add category
        })
    
    # Write updated notifications to the database
    if notifications:
        notifications_df = pd.DataFrame(notifications)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS dashboard_notifications (
                type TEXT,
                notification TEXT,
                store_location TEXT,
                event_timestamp TEXT,
                product_id TEXT,
                category TEXT  -- Ensure category column is included
            )
        ''')
        notifications_df.to_sql('dashboard_notifications', conn, if_exists='replace', index=False)
        print_and_log(f"Updated {len(notifications)} notifications in dashboard_notifications table.")
    else:
        print_and_log("No notifications to update.")

def main():
    print_and_log("Starting detailed granularity update...")
    
    # Connect to the database
    conn = sqlite3.connect('D:\\inventory_project\\data\\ml_metrics.db')
    
    try:
        # Migrate the dashboard_notifications table if needed
        migrate_notifications_table(conn)
        
        # Update anomalies table
        anomalies_updated = replace_all_with_granular(conn, 'anomalies')
        numerical_anomalies_updated = replace_all_with_granular(conn, 'numerical_anomalies')
        
        if not (anomalies_updated or numerical_anomalies_updated):
            print_and_log("No 'All' values found to replace in anomalies or numerical_anomalies tables.")
            return
        
        # Update notifications
        update_notifications(conn)
        print_and_log("Detailed granularity update completed successfully.")
    
    except Exception as e:
        print_and_log(f"ERROR: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()