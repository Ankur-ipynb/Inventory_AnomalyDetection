import logging
import sqlite3
import pandas as pd
import sys
import time

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\post_process_metrics.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_and_log(msg):
    print(msg)
    logging.info(msg)

# Main processing logic
def main():
    print_and_log("Starting post_process_metrics.py...")
    start_time = time.time()

    # Connect to SQLite metrics DB with retry logic
    conn = None
    max_retries = 3
    retry_delay = 5
    db_path = 'D:\\inventory_project\\data\\inventory_metrics.db'
    use_in_memory_db = False

    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(db_path, timeout=60)
            # Set journal mode to WAL for better concurrency
            conn.execute('PRAGMA journal_mode=WAL;')
            journal_mode = conn.execute('PRAGMA journal_mode;').fetchone()[0]
            if journal_mode.lower() != 'wal':
                raise Exception(f"Failed to set journal mode to WAL, got {journal_mode}")
            print_and_log(f"Journal mode set to {journal_mode}.")
            break
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                print_and_log(f"Database is locked, retrying ({attempt+1}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                print_and_log(f"Failed to connect to SQLite metrics DB: {e}")
                raise
        except Exception as e:
            print_and_log(f"Failed to connect to SQLite metrics DB: {e}")
            if attempt == max_retries - 1:
                print_and_log("Max retries reached. Switching to in-memory SQLite database (fault tolerance activated).")
                use_in_memory_db = True
                break
            time.sleep(retry_delay)

    # Fault tolerance: Use in-memory SQLite if file-based DB fails
    if use_in_memory_db:
        try:
            conn = sqlite3.connect(":memory:")
            print_and_log("In-memory SQLite metrics DB ready.")
        except Exception as e:
            print_and_log(f"ERROR: Failed to set up in-memory SQLite metrics DB: {e}")
            raise

    try:
        print_and_log("Reading metrics data...")
        df = pd.read_sql_query("SELECT * FROM metrics", conn)
        print_and_log(f"Read {len(df)} rows from metrics DB.")

        if df.empty:
            print_and_log("ERROR: Metrics DataFrame is empty. Cannot proceed with post-processing.")
            raise ValueError("Metrics DataFrame is empty.")

        # Log DataFrame info for debugging
        print_and_log("DataFrame info:\n" + str(df.dtypes))

        # Convert columns to numeric types before computation
        numeric_columns = ['total_quantity', 'stock_value_by_category', 'total_stock_value']
        print_and_log(f"Converting columns {numeric_columns} to numeric types...")
        try:
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print_and_log(f"Converted {col} to numeric. Sample values:\n{df[col].head().to_string()}")
        except Exception as e:
            print_and_log(f"ERROR: Failed to convert columns to numeric: {e}")
            raise

        # Check for NaN values after conversion
        for col in numeric_columns:
            if df[col].isna().any():
                print_and_log(f"WARNING: Some values in {col} could not be converted to numeric. They will be filled with 0.")
                df[col] = df[col].fillna(0)

        # Compute moving average stock value (window of 5)
        print_and_log("Computing moving average stock value...")
        try:
            df['moving_avg_stock_value'] = df['total_stock_value'].rolling(window=5, min_periods=1).mean()
            print_and_log("Moving average computed successfully.")
        except Exception as e:
            print_and_log(f"ERROR: Failed to compute moving average: {e}")
            raise

        # Compute turnover rate (simplified: total_quantity / (total_quantity + stock_value_by_category))
        print_and_log("Computing turnover rate...")
        try:
            denominator = df['total_quantity'] + df['stock_value_by_category']
            df['turnover_rate'] = df['total_quantity'] / (denominator + 1e-10)  # Add small epsilon to avoid division by zero
            print_and_log("Turnover rate computed successfully.")
        except Exception as e:
            print_and_log(f"ERROR: Failed to compute turnover rate: {e}")
            raise

        # Write back to the database
        print_and_log("Writing updated metrics back to SQLite...")
        try:
            df.to_sql('metrics', conn, if_exists='replace', index=False)
            print_and_log("Updated metrics written to SQLite.")
        except Exception as e:
            print_and_log(f"ERROR: Failed to write updated metrics to SQLite: {e}")
            raise

    except Exception as e:
        print_and_log(f"ERROR in post_process_metrics.py: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print_and_log("SQLite metrics DB connection closed in post_process_metrics.py.")

    print_and_log(f"post_process_metrics.py finished in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        sys.exit(1)