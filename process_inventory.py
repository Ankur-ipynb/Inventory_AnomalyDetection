import logging
import sqlite3
import pandas as pd
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum, avg, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pulsar import Client as PulsarClient
import json
import time
from io import StringIO
import gc

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\process_inventory.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_and_log(msg):
    print(msg)
    logging.info(msg)

# Debug: Log SQLite connection status
def log_sqlite_connections():
    try:
        print_and_log(f"SQLite version: {sqlite3.sqlite_version}")
    except Exception as e:
        print_and_log(f"ERROR: Failed to log SQLite connection status: {e}")

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

# Read data from Pulsar
use_pulsar = True
pandas_df = None
try:
    pulsar_client = PulsarClient('pulsar://172.27.235.96:6650', operation_timeout_seconds=30)
    consumer = pulsar_client.subscribe('persistent://public/default/inventory-topic', 'spark-subscription')

    messages = []
    max_messages = 20
    timeout_ms = 5000

    for i in range(max_messages):
        try:
            msg = consumer.receive(timeout_millis=timeout_ms)
            data = json.loads(msg.data().decode('utf-8'))
            messages.append(data)
            consumer.acknowledge(msg)
            print_and_log(f"Read message {i+1}: {data}")
        except Exception as e:
            print_and_log(f"No more messages available after {i} messages: {e}")
            break

    pandas_df = pd.DataFrame(messages)
    if pandas_df.empty:
        raise Exception("No data read from Pulsar.")
    print_and_log(f"Successfully read {len(pandas_df)} rows from Pulsar.")
except Exception as e:
    print_and_log(f"Pulsar connection failed: {e}")
    use_pulsar = False

# SQLite fallback
if not use_pulsar:
    conn = None
    try:
        conn = sqlite3.connect('D:\\inventory_project\\data\\inventory.db', timeout=30)
        pandas_df = pd.read_sql_query("SELECT * FROM inventory", conn)
        print_and_log(f"Read {len(pandas_df)} rows from SQLite inventory.db.")
        if pandas_df.empty:
            print_and_log("SQLite inventory DB is empty.")
            sys.exit(1)
        print_and_log("Loaded fallback data from SQLite.")
    except Exception as e:
        print_and_log(f"Fallback to SQLite failed: {e}")
        sys.exit(1)
    finally:
        if conn:
            try:
                conn.close()
                print_and_log("SQLite inventory.db connection closed.")
            except Exception as e:
                print_and_log(f"ERROR: Failed to close SQLite inventory.db connection: {e}")

# Debug: Log the data types and sample of the DataFrame
print_and_log(f"Pandas DataFrame dtypes:\n{pandas_df.dtypes}")
print_and_log(f"Pandas DataFrame sample:\n{pandas_df.head().to_string()}")

# Clean and ensure event_timestamp is a string
pandas_df['event_timestamp'] = pandas_df['event_timestamp'].astype(str).replace('nan', '')  # Handle NaN values
# Verify no non-string values remain
if not pandas_df['event_timestamp'].apply(lambda x: isinstance(x, str)).all():
    print_and_log("ERROR: event_timestamp column contains non-string values after conversion.")
    sys.exit(1)

# Define the schema explicitly to avoid inference overhead
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("store_location", StringType(), True),
    StructField("category", StringType(), True)
])

# Reorder Pandas DataFrame columns to match the schema
schema_columns = [field.name for field in schema.fields]
print_and_log(f"Schema columns order: {schema_columns}")
print_and_log(f"Pandas DataFrame columns before reordering: {list(pandas_df.columns)}")
pandas_df = pandas_df[schema_columns]  # Reorder columns to match schema
print_and_log(f"Pandas DataFrame columns after reordering: {list(pandas_df.columns)}")

# Initialize Spark session with optimized settings
print_and_log("Starting Spark session initialization...")
start_time = time.time()
spark_init_timeout = 120  # 2 minutes timeout for Spark session creation
spark = None
try:
    spark = SparkSession.builder \
        .appName("InventoryProcessing") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.python.worker.start.timeout", "60s") \
        .config("spark.ui.port", "4040") \
        .config("spark.ui.enabled", "true") \
        .config("spark.driver.host", "localhost") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
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
    print_and_log(f"Spark environment initialized in {time.time() - start_time:.2f} seconds.")
except Exception as e:
    if time.time() - start_time > spark_init_timeout:
        print_and_log(f"ERROR: Spark session initialization timed out after {spark_init_timeout} seconds.")
        sys.exit(1)
    else:
        print_and_log(f"ERROR: Failed to initialize Spark session: {e}")
        sys.exit(1)

# Debug: Log Spark configuration
print_and_log("Spark configuration details:")
for conf in spark.sparkContext.getConf().getAll():
    print_and_log(f"  {conf[0]}: {conf[1]}")

# Log Spark UI availability
print_and_log(f"Spark UI should be available at http://localhost:4040")

# Main processing block with proper cleanup
try:
    # Load data into Spark with explicit schema
    print_and_log("Converting Pandas DataFrame to Spark DataFrame with explicit schema...")
    start_time = time.time()
    df = spark.createDataFrame(pandas_df, schema=schema) \
              .withColumn("ts", to_timestamp(col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    print_and_log(f"Spark DataFrame created with {df.count()} rows in {time.time() - start_time:.2f} seconds.")

    # Cache the DataFrame to avoid recomputation
    print_and_log("Caching Spark DataFrame...")
    df.cache()
    print_and_log(f"DataFrame cached. Storage level: {df.storageLevel}")

    # Debug: Sample the event_timestamp and ts columns as Pandas DataFrame
    print_and_log("Debug: Sampling event_timestamp and ts columns as Pandas DataFrame...")
    max_retries = 3
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            pandas_sample = df.select("event_timestamp", "ts").limit(5).toPandas()
            print_and_log("Sample data:\n" + pandas_sample.to_string())
            break
        except Exception as e:
            if "Broken pipe" in str(e):
                print_and_log(f"Broken pipe error during toPandas(), retrying ({attempt+1}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                print_and_log(f"ERROR: Failed to sample event_timestamp and ts columns: {e}. Skipping display.")
                break
    else:
        print_and_log("Max retries reached. Failed to sample event_timestamp and ts columns due to broken pipe.")

    # Ensure metrics DB table exists with fault tolerance
    max_retries = 3
    retry_delay = 5  # seconds
    use_in_memory_db = False
    db_path = 'D:\\inventory_project\\data\\inventory_metrics.db'
    conn_metrics = None

    print_and_log("Setting up SQLite metrics DB...")
    start_time = time.time()
    for attempt in range(max_retries):
        try:
            conn_metrics = sqlite3.connect(db_path, timeout=60)  # Increased timeout
            # Set journal mode to WAL
            conn_metrics.execute('PRAGMA journal_mode=WAL;')
            journal_mode = conn_metrics.execute('PRAGMA journal_mode;').fetchone()[0]
            if journal_mode.lower() != 'wal':
                raise Exception(f"Failed to set journal mode to WAL, got {journal_mode}")
            print_and_log(f"Journal mode set to {journal_mode}.")

            # Create the metrics table
            conn_metrics.execute('''
                CREATE TABLE IF NOT EXISTS metrics (
                    store_location TEXT,
                    category TEXT,
                    event_timestamp TEXT,
                    total_quantity INTEGER,
                    avg_price REAL,
                    total_stock_value REAL,
                    moving_avg_stock_value REAL,
                    turnover_rate REAL,
                    stock_value_by_category REAL
                )
            ''')
            conn_metrics.commit()
            print_and_log(f"SQLite metrics DB ready in {time.time() - start_time:.2f} seconds.")
            break  # Exit retry loop on success
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                print_and_log(f"Database is locked, retrying ({attempt+1}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                print_and_log(f"Failed to set up SQLite metrics DB: {e}")
                raise
        except Exception as e:
            print_and_log(f"Failed to set up SQLite metrics DB: {e}")
            if attempt == max_retries - 1:
                print_and_log("Max retries reached. Switching to in-memory SQLite database (fault tolerance activated).")
                use_in_memory_db = True
                break
            time.sleep(retry_delay)
        finally:
            if conn_metrics:
                try:
                    conn_metrics.close()
                    print_and_log("SQLite metrics DB connection closed after setup.")
                except Exception as e:
                    print_and_log(f"ERROR: Failed to close SQLite metrics DB connection: {e}")

    # Fault tolerance: Use in-memory SQLite if file-based DB fails
    if use_in_memory_db:
        print_and_log("Fault tolerance activated: Using in-memory SQLite database.")
        db_path = ":memory:"
        conn_metrics = None
        try:
            conn_metrics = sqlite3.connect(db_path)
            conn_metrics.execute('''
                CREATE TABLE metrics (
                    store_location TEXT,
                    category TEXT,
                    event_timestamp TEXT,
                    total_quantity INTEGER,
                    avg_price REAL,
                    total_stock_value REAL,
                    moving_avg_stock_value REAL,
                    turnover_rate REAL,
                    stock_value_by_category REAL
                )
            ''')
            conn_metrics.commit()
            print_and_log("In-memory SQLite metrics DB ready.")
        except Exception as e:
            print_and_log(f"ERROR: Failed to set up in-memory SQLite metrics DB: {e}")
            raise
        finally:
            if conn_metrics:
                try:
                    conn_metrics.close()
                    print_and_log("In-memory SQLite metrics DB connection closed.")
                except Exception as e:
                    print_and_log(f"ERROR: Failed to close in-memory SQLite metrics DB connection: {e}")

    # Process the data
    print_and_log("Calculating stock value for base data...")
    start_time = time.time()
    base_data = df.withColumn("stock_value", col("quantity") * col("price"))
    print_and_log(f"Base data prepared with {base_data.count()} rows in {time.time() - start_time:.2f} seconds.")

    # Cache base_data
    print_and_log("Caching base_data...")
    base_data.cache()
    print_and_log(f"base_data cached. Storage level: {base_data.storageLevel}")

    # Debug: Sample base_data using Pandas
    print_and_log("Debug: Sampling base_data as Pandas DataFrame...")
    for attempt in range(max_retries):
        try:
            pandas_sample = base_data.limit(5).toPandas()
            print_and_log("Sample data:\n" + pandas_sample.to_string())
            break
        except Exception as e:
            if "Broken pipe" in str(e):
                print_and_log(f"Broken pipe error during toPandas(), retrying ({attempt+1}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                print_and_log(f"ERROR: Failed to sample base_data: {e}. Skipping display.")
                break
    else:
        print_and_log("Max retries reached. Failed to sample base_data due to broken pipe.")

    # Windowed aggregation
    print_and_log("Computing windowed aggregation...")
    start_time = time.time()
    windowed_data = base_data.groupBy(
        col("product_id"),
        col("store_location"),
        col("category"),
        window(col("ts"), "1 hour").alias("window")
    ).agg(
        sum("quantity").alias("total_quantity"),
        avg("price").alias("avg_price"),
        sum("stock_value").alias("total_stock_value")
    ).select(
        col("product_id"),
        col("store_location"),
        col("category"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_quantity"),
        col("avg_price"),
        col("total_stock_value")
    )
    print_and_log(f"Windowed data computed with {windowed_data.count()} rows in {time.time() - start_time:.2f} seconds.")

    # Debug: Sample windowed_data using Pandas
    if windowed_data.count() > 0:
        print_and_log("Debug: Sampling windowed_data as Pandas DataFrame...")
        for attempt in range(max_retries):
            try:
                pandas_sample = windowed_data.limit(5).toPandas()
                print_and_log("Sample data:\n" + pandas_sample.to_string())
                break
            except Exception as e:
                if "Broken pipe" in str(e):
                    print_and_log(f"Broken pipe error during toPandas(), retrying ({attempt+1}/{max_retries})...")
                    time.sleep(retry_delay)
                else:
                    print_and_log(f"ERROR: Failed to sample windowed_data: {e}. Skipping display.")
                    break
        else:
            print_and_log("Max retries reached. Failed to sample windowed_data due to broken pipe.")
    else:
        print_and_log("Debug: Windowed data is empty. Trying a simpler aggregation...")
        start_time = time.time()
        simple_agg = base_data.groupBy(
            col("product_id"),
            col("store_location"),
            col("category")
        ).agg(
            sum("quantity").alias("total_quantity"),
            avg("price").alias("avg_price"),
            sum("stock_value").alias("total_stock_value")
        )
        print_and_log(f"Simple aggregation computed with {simple_agg.count()} rows in {time.time() - start_time:.2f} seconds.")
        for attempt in range(max_retries):
            try:
                pandas_sample = simple_agg.limit(5).toPandas()
                print_and_log("Sample data:\n" + pandas_sample.to_string())
                break
            except Exception as e:
                if "Broken pipe" in str(e):
                    print_and_log(f"Broken pipe error during toPandas(), retrying ({attempt+1}/{max_retries})...")
                    time.sleep(retry_delay)
                else:
                    print_and_log(f"ERROR: Failed to sample simple_agg: {e}. Skipping display.")
                    break
        else:
            print_and_log("Max retries reached. Failed to sample simple_agg due to broken pipe.")
        windowed_data = simple_agg.select(
            col("product_id"),
            col("store_location"),
            col("category"),
            col("total_quantity"),
            col("avg_price"),
            col("total_stock_value")
        )

    # Compute stock value by category
    print_and_log("Computing stock value by category...")
    start_time = time.time()
    category_stock = windowed_data.groupBy("category") \
        .agg(sum("total_stock_value").alias("stock_value_by_category"))
    print_and_log(f"Category stock computed with {category_stock.count()} rows in {time.time() - start_time:.2f} seconds.")

    # Join the data
    print_and_log("Joining windowed data with category stock...")
    start_time = time.time()
    result = windowed_data.join(category_stock, "category")
    if "window_start" in [col.name for col in windowed_data.schema.fields]:
        result = result.select(
            col("product_id"),
            col("store_location"),
            col("category"),
            col("window_start").alias("event_timestamp"),
            col("total_quantity"),
            col("avg_price"),
            col("total_stock_value"),
            col("stock_value_by_category")
        )
    else:
        # If windowed aggregation failed, use the original event_timestamp from base_data
        result = result.join(
            base_data.select("product_id", "store_location", "category", "event_timestamp"),
            ["product_id", "store_location", "category"],
            "left"
        ).select(
            col("product_id"),
            col("store_location"),
            col("category"),
            col("event_timestamp"),
            col("total_quantity"),
            col("avg_price"),
            col("total_stock_value"),
            col("stock_value_by_category")
        )
    print_and_log(f"Final result computed with {result.count()} rows in {time.time() - start_time:.2f} seconds.")

    # Debug: Show result using Pandas
    if result.count() > 0:
        print_and_log("Debug: Showing final result schema:")
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        result.printSchema()
        schema_output = sys.stdout.getvalue()
        sys.stdout = old_stdout
        print_and_log("Schema:\n" + schema_output)
        
        print_and_log("Debug: Sampling final result as Pandas DataFrame...")
        for attempt in range(max_retries):
            try:
                pandas_sample = result.limit(5).toPandas()
                print_and_log("Sample data:\n" + pandas_sample.to_string())
                break
            except Exception as e:
                if "Broken pipe" in str(e):
                    print_and_log(f"Broken pipe error during toPandas(), retrying ({attempt+1}/{max_retries})...")
                    time.sleep(retry_delay)
                else:
                    print_and_log(f"ERROR: Failed to sample final result: {e}. Skipping display.")
                    break
        else:
            print_and_log("Max retries reached. Failed to sample final result due to broken pipe.")
    else:
        print_and_log("Debug: Final result is empty.")

    # Write to SQLite with optimized batch insertion
    print_and_log("Converting Spark DataFrame to Pandas for SQLite insertion...")
    start_time = time.time()
    pandas_df = None
    for attempt in range(max_retries):
        try:
            pandas_df = result.toPandas()
            print_and_log(f"Converted to Pandas DataFrame with {len(pandas_df)} rows in {time.time() - start_time:.2f} seconds.")
            break
        except Exception as e:
            if "Broken pipe" in str(e):
                print_and_log(f"Broken pipe error during toPandas(), retrying ({attempt+1}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                print_and_log(f"ERROR: Failed to convert to Pandas: {e}")
                raise
    else:
        print_and_log("Max retries reached. Failed to convert to Pandas due to broken pipe.")
        sys.exit(1)

    # Ensure columns are numeric before insertion
    print_and_log("Ensuring columns are numeric before SQLite insertion...")
    try:
        pandas_df['total_quantity'] = pd.to_numeric(pandas_df['total_quantity'], errors='coerce').fillna(0).astype(int)
        pandas_df['stock_value_by_category'] = pd.to_numeric(pandas_df['stock_value_by_category'], errors='coerce').fillna(0).astype(float)
        pandas_df['avg_price'] = pd.to_numeric(pandas_df['avg_price'], errors='coerce').fillna(0).astype(float)
        pandas_df['total_stock_value'] = pd.to_numeric(pandas_df['total_stock_value'], errors='coerce').fillna(0).astype(float)
    except Exception as e:
        print_and_log(f"ERROR: Failed to convert columns to numeric before insertion: {e}")
        raise

    max_retries = 5
    retry_delay = 10  # Increased retry delay
    conn_metrics = None

    print_and_log("Inserting data into SQLite metrics DB...")
    start_time = time.time()
    for attempt in range(max_retries):
        try:
            conn_metrics = sqlite3.connect(db_path, timeout=120)  # Increased timeout
            cursor = conn_metrics.cursor()
            # Clear existing data
            cursor.execute("DELETE FROM metrics")
            # Prepare batch insertion
            records = []
            for idx, row in pandas_df.iterrows():
                event_timestamp = row.get('event_timestamp')
                if pd.notnull(event_timestamp):
                    event_timestamp = event_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                records.append((
                    row['store_location'],
                    row['category'],
                    event_timestamp,
                    row['total_quantity'],
                    row['avg_price'],
                    row['total_stock_value'],
                    None,  # moving_avg_stock_value (placeholder)
                    None,  # turnover_rate (placeholder)
                    row['stock_value_by_category']
                ))
            # Batch insert
            cursor.executemany("""
                INSERT INTO metrics (
                    store_location, category, event_timestamp,
                    total_quantity, avg_price, total_stock_value,
                    moving_avg_stock_value, turnover_rate, stock_value_by_category
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            conn_metrics.commit()
            print_and_log(f"Metrics inserted into SQLite in {time.time() - start_time:.2f} seconds.")
            break  # Exit retry loop on success
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                print_and_log(f"Database is locked, retrying ({attempt+1}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                print_and_log(f"Failed to insert metrics: {e}")
                raise
        except Exception as e:
            print_and_log(f"Failed to insert metrics: {e}")
            raise
        finally:
            if conn_metrics:
                try:
                    conn_metrics.close()
                    print_and_log("SQLite metrics DB connection closed after insertion.")
                except Exception as e:
                    print_and_log(f"ERROR: Failed to close SQLite metrics DB connection: {e}")
    else:
        print_and_log("Max retries reached. Failed to insert metrics due to database lock.")
        sys.exit(1)

    # Log completion of processing
    print_and_log("Inventory processing completed successfully.")

except Exception as e:
    print_and_log(f"ERROR: An unexpected error occurred: {e}")
    raise
finally:
    # Ensure Spark session is stopped
    if spark:
        try:
            # Unpersist cached DataFrames
            for table in spark.catalog.listTables():
                spark.catalog.dropTempView(table.name)
            spark.catalog.clearCache()
            print_and_log("Cleared Spark cache and temp views.")
            
            # Stop the Spark session
            spark.sparkContext.stop()
            spark.stop()
            print_and_log("Spark session stopped.")
            
            # Force garbage collection
            gc.collect()
            print_and_log("Garbage collection performed.")
            
            # Small delay to ensure all resources are released
            time.sleep(2)
            print_and_log("Process cleanup completed.")
        except Exception as e:
            print_and_log(f"ERROR: Failed to stop Spark session: {e}")
            raise
    else:
        print_and_log("No Spark session to stop.")
    
    # Ensure the script exits cleanly
    sys.stdout.flush()
    sys.stderr.flush()
    print_and_log("Script execution completed. Exiting.")
    sys.exit(0)