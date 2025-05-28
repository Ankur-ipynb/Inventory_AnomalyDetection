import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import subprocess
import time
import os
import logging
import re
from datetime import datetime
from query import handle_user_query  # Import the renamed query handler

# Set up logging
logging.basicConfig(
    filename='D:\\inventory_project\\logs\\dashboard.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_and_log(msg):
    print(msg)
    logging.info(msg)

# Set page config
st.set_page_config(page_title="Inventory Dashboard", layout="wide")

# Title
st.title("Inventory Dashboard")
# Update the Last Updated timestamp to current time
current_time = datetime.now().strftime("%I:%M %p IST, %B %d, %Y")
st.markdown(f"**Last Updated:** {current_time}")

# Connect to SQLite databases
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

# Load raw inventory data
def load_raw_data():
    conn = get_db_connection('D:/inventory_project/data/inventory.db')
    if not table_exists(conn, 'inventory'):
        conn.close()
        return pd.DataFrame()
    df = pd.read_sql_query("SELECT * FROM inventory", conn)
    conn.close()
    return df

# Load metrics data
def load_metrics_data():
    conn = get_db_connection('D:/inventory_project/data/inventory_metrics.db')
    if not table_exists(conn, 'metrics'):
        conn.close()
        return pd.DataFrame()
    df = pd.read_sql_query("SELECT * FROM metrics", conn)
    conn.close()
    return df

# Load ML metrics data
def load_ml_metrics_data(model_name):
    conn = get_db_connection('D:/inventory_project/data/ml_metrics.db')
    if not table_exists(conn, 'ml_metrics'):
        conn.close()
        return pd.DataFrame()
    df = pd.read_sql_query(f"SELECT * FROM ml_metrics WHERE model_name = '{model_name}'", conn)
    conn.close()
    # Convert actual_quantity from bytes to integer if necessary
    if 'actual_quantity' in df.columns:
        df['actual_quantity'] = df['actual_quantity'].apply(
            lambda x: int.from_bytes(x, byteorder='little') if isinstance(x, bytes) else x
        )
    return df

# Function to read the entire log file content
def read_log_file(log_file):
    if not os.path.exists(log_file):
        return ""
    with open(log_file, 'r') as f:
        content = f.read()
    return content

# Function to estimate progress based on log messages
def estimate_progress(log_message):
    progress_steps = {
        "Environment variables set": 5,
        "winutils.exe found": 7,
        "Loaded fallback data from SQLite": 10,
        "Starting Spark session initialization": 15,
        "Spark environment initialized": 20,
        "Converting Pandas DataFrame to Spark DataFrame": 25,
        "Spark DataFrame created": 30,
        "Caching Spark DataFrame": 35,
        "DataFrame cached": 40,
        "Setting up SQLite metrics DB": 45,
        "SQLite metrics DB ready": 50,
        "Calculating stock value for base data": 55,
        "Base data prepared": 60,
        "Caching base_data": 65,
        "base_data cached": 70,
        "Computing windowed aggregation": 75,
        "Windowed data computed": 80,
        "Computing stock value by category": 85,
        "Category stock computed": 87,
        "Joining windowed data with category stock": 90,
        "Final result computed": 92,
        "Converting Spark DataFrame to Pandas": 94,
        "Inserting data into SQLite metrics DB": 96,
        "Metrics inserted into SQLite": 98,
        "Inventory processing completed successfully": 100
    }
    for key, value in progress_steps.items():
        if key in log_message:
            return value
    return None

# Function to estimate ML progress based on log messages
def estimate_ml_progress(log_message):
    progress_steps = {
        "Starting ML model": 10,
        "Spark session initialized": 20,
        "Loaded data from SQLite": 30,
        "DataFrame created": 40,
        "Training model": 60,
        "Model training completed": 70,
        "Generating predictions": 80,
        "Writing predictions to SQLite": 90,
        "ML processing completed": 100
    }
    for key, value in progress_steps.items():
        if key in log_message:
            return value
    return None

# Process metrics without displaying results
def process_metrics():
    timeout_seconds = 600  # 10 minutes
    log_file = 'D:/inventory_project/logs/process_inventory.log'
    
    # Clear the log file to start fresh
    if os.path.exists(log_file):
        open(log_file, 'w').close()
    
    # Use the virtual environment's Python executable
    venv_python = "D:\\venv\\Scripts\\python.exe"
    
    # Progress bar and log message display
    progress_bar = st.progress(0)
    status_container = st.container()
    fault_tolerance_message = status_container.empty()
    log_message = status_container.empty()
    
    with st.expander("View Processing Logs", expanded=True):
        log_placeholder = st.empty()
        log_placeholder.code("", language="plaintext", line_numbers=True)
    
    start_time = time.time()
    fault_tolerance_activated = False
    log_content = []
    
    # Run the subprocess with a timeout
    try:
        result = subprocess.run(
            [venv_python, "D:/inventory_project/notebooks/process_inventory.py"],
            capture_output=True,
            text=True,
            timeout=timeout_seconds
        )
        # Capture logs immediately after completion
        full_log = read_log_file(log_file)
        if full_log:
            log_lines = full_log.strip().split('\n')
            log_content = log_lines
            latest_message = log_lines[-1] if log_lines else ""
        else:
            latest_message = "No logs available."
        
        # Update log display
        if log_content:
            log_placeholder.code(
                "\n".join(log_content[-50:]),
                language="plaintext",
                line_numbers=True
            )
        
        # Check for fault tolerance
        if "Fault tolerance activated" in full_log:
            fault_tolerance_activated = True
            fault_tolerance_message.markdown("**Fault tolerance activated permanently.**")
        
        # Update progress
        progress = estimate_progress(latest_message)
        if progress is not None:
            progress_bar.progress(progress)
        
        # Check subprocess result
        if result.returncode != 0:
            error_message = result.stderr
            log_message.markdown(f"**<span style='color:red'>Processing: {error_message}</span>**", unsafe_allow_html=True)
            log_placeholder.code(
                "\n".join(log_content[-50:] + [f"ERROR: {error_message}"]),
                language="plaintext",
                line_numbers=True
            )
            raise Exception(error_message)
        
        progress_bar.progress(100)
        log_message.markdown("Processing: Complete!")
    
    except subprocess.TimeoutExpired:
        # If timeout occurs, check logs to determine if processing was successful
        full_log = read_log_file(log_file)
        if full_log:
            log_lines = full_log.strip().split('\n')
            log_content = log_lines
            latest_message = log_lines[-1] if log_lines else "No logs available."
        else:
            latest_message = "No logs available after timeout."
        
        log_placeholder.code(
            "\n".join(log_content[-50:]),
            language="plaintext",
            line_numbers=True
        )
        
        if "Fault tolerance activated" in full_log:
            fault_tolerance_activated = True
            fault_tolerance_message.markdown("**Fault tolerance activated permanently.**")
        
        if "Inventory processing completed successfully" in full_log:
            # Consider the process successful if the log indicates completion
            progress_bar.progress(100)
            log_message.markdown("Processing: Complete (despite timeout)!")
            return True  # Indicate success
        else:
            error_message = "Processing timed out after 10 minutes. Please check the logs for errors."
            log_message.markdown(f"**<span style='color:red'>Processing: {error_message}</span>**", unsafe_allow_html=True)
            log_placeholder.code(
                "\n".join(log_content[-50:] + [f"ERROR: {error_message}"]),
                language="plaintext",
                line_numbers=True
            )
            raise Exception(error_message)
    
    except Exception as e:
        log_message.markdown(f"**<span style='color:red'>Processing: {str(e)}</span>**", unsafe_allow_html=True)
        raise
    
    return True  # Indicate success

# Ingest more data from Pulsar
def ingest_data():
    timeout_seconds = 300  # 5 minutes
    log_file = 'D:/inventory_project/logs/ingest.log'
    
    # Clear the log file to start fresh
    if os.path.exists(log_file):
        open(log_file, 'w').close()
    
    # Use the virtual environment's Python executable
    venv_python = "D:\\venv\\Scripts\\python.exe"
    
    # Progress bar and log message display
    progress_bar = st.progress(0)
    status_container = st.container()
    log_message = status_container.empty()
    
    with st.expander("View Ingestion Logs", expanded=True):
        log_placeholder = st.empty()
        log_placeholder.code("", language="plaintext", line_numbers=True)
    
    log_content = []
    
    try:
        result = subprocess.run(
            [venv_python, "D:/inventory_project/notebooks/ingest.py"],
            capture_output=True,
            text=True,
            timeout=timeout_seconds
        )
        # Capture logs immediately after completion
        full_log = read_log_file(log_file)
        if full_log:
            log_lines = full_log.strip().split('\n')
            log_content = log_lines
        else:
            log_content = ["No logs available."]
        
        # Update log display
        log_placeholder.code(
            "\n".join(log_content[-50:]),
            language="plaintext",
            line_numbers=True
        )
        
        # Check subprocess result
        if result.returncode != 0:
            error_message = result.stderr
            log_message.markdown(f"**<span style='color:red'>Ingestion: {error_message}</span>**", unsafe_allow_html=True)
            log_placeholder.code(
                "\n".join(log_content[-50:] + [f"ERROR: {error_message}"]),
                language="plaintext",
                line_numbers=True
            )
            raise Exception(error_message)
        
        progress_bar.progress(100)
        log_message.markdown("Ingestion: Complete!")
        st.success("Ingestion Successful!")
    
    except subprocess.TimeoutExpired:
        full_log = read_log_file(log_file)
        if full_log:
            log_lines = full_log.strip().split('\n')
            log_content = log_lines
        else:
            log_content = ["No logs available after timeout."]
        
        log_placeholder.code(
            "\n".join(log_content[-50:]),
            language="plaintext",
            line_numbers=True
        )
        
        error_message = "Ingestion timed out after 5 minutes. Please check the logs for errors."
        log_message.markdown(f"**<span style='color:red'>Ingestion: {error_message}</span>**", unsafe_allow_html=True)
        log_placeholder.code(
            "\n".join(log_content[-50:] + [f"ERROR: {error_message}"]),
            language="plaintext",
            line_numbers=True
        )
        raise Exception(error_message)
    
    except Exception as e:
        log_message.markdown(f"**<span style='color:red'>Ingestion: {str(e)}</span>**", unsafe_allow_html=True)
        raise
    
    return True  # Indicate success

# Run ML model
def run_ml_model(model_name):
    timeout_seconds = 600  # 10 minutes
    log_file = f'D:/inventory_project/logs/{model_name.lower()}_model.log'
    
    # Clear the log file to start fresh
    if os.path.exists(log_file):
        open(log_file, 'w').close()
    
    # Use the virtual environment's Python executable
    venv_python = "D:\\venv\\Scripts\\python.exe"
    script_path = f"D:/inventory_project/notebooks/{model_name.lower()}_model.py"
    
    # Progress bar and log message display
    progress_bar = st.progress(0)
    status_container = st.container()
    log_message = status_container.empty()
    
    with st.expander(f"View {model_name} Model Logs", expanded=True):
        log_placeholder = st.empty()
        log_placeholder.code("", language="plaintext", line_numbers=True)
    
    log_content = []
    
    try:
        result = subprocess.run(
            [venv_python, script_path],
            capture_output=True,
            text=True,
            timeout=timeout_seconds
        )
        # Capture logs immediately after completion
        full_log = read_log_file(log_file)
        if full_log:
            log_lines = full_log.strip().split('\n')
            log_content = log_lines
            latest_message = log_lines[-1] if log_lines else ""
        else:
            latest_message = "No logs available."
        
        # Update log display
        if log_content:
            log_placeholder.code(
                "\n".join(log_content[-50:]),
                language="plaintext",
                line_numbers=True
            )
        
        # Update progress
        progress = estimate_ml_progress(latest_message)
        if progress is not None:
            progress_bar.progress(progress)
        
        # Check subprocess result
        if result.returncode != 0:
            error_message = result.stderr
            log_message.markdown(f"**<span style='color:red'>{model_name} Model: {error_message}</span>**", unsafe_allow_html=True)
            log_placeholder.code(
                "\n".join(log_content[-50:] + [f"ERROR: {error_message}"]),
                language="plaintext",
                line_numbers=True
            )
            raise Exception(error_message)
        
        progress_bar.progress(100)
        log_message.markdown(f"{model_name} Model: Complete!")
    
    except subprocess.TimeoutExpired:
        full_log = read_log_file(log_file)
        if full_log:
            log_lines = full_log.strip().split('\n')
            log_content = log_lines
            latest_message = log_lines[-1] if log_lines else "No logs available."
        else:
            latest_message = "No logs available after timeout."
        
        log_placeholder.code(
            "\n".join(log_content[-50:]),
            language="plaintext",
            line_numbers=True
        )
        
        if "ML processing completed" in full_log:
            progress_bar.progress(100)
            log_message.markdown(f"{model_name} Model: Complete (despite timeout)!")
            return True
        else:
            error_message = f"{model_name} model processing timed out after 10 minutes. Please check the logs for errors."
            log_message.markdown(f"**<span style='color:red'>{model_name} Model: {error_message}</span>**", unsafe_allow_html=True)
            log_placeholder.code(
                "\n".join(log_content[-50:] + [f"ERROR: {error_message}"]),
                language="plaintext",
                line_numbers=True
            )
            raise Exception(error_message)
    
    except Exception as e:
        log_message.markdown(f"**<span style='color:red'>{model_name} Model: {str(e)}</span>**", unsafe_allow_html=True)
        raise
    
    return True  # Indicate success

# View ML metrics (runs view_ml_metrics.py and displays visualizations)
def view_ml_metrics(model_name):
    log_file = 'D:/inventory_project/logs/view_ml_metrics.log'
    if os.path.exists(log_file):
        with open(log_file, 'a') as f:
            f.write(f"\n--- Starting View ML Metrics for {model_name} ---\n")
    
    with st.spinner("Running ML metrics processing..."):
        try:
            result = subprocess.run(
                ["D:\\venv\\Scripts\\python.exe", "D:/inventory_project/notebooks/view_ml_metrics.py", model_name],
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode != 0:
                st.error(f"ML metrics processing failed with return code {result.returncode}: {result.stderr}")
                return
            st.success("ML metrics processing completed successfully.")
        except subprocess.TimeoutExpired:
            st.error("ML metrics processing timed out after 5 minutes.")
            return
        except Exception as e:
            st.error(f"Error during ML metrics processing: {e}")
            return

    with st.spinner("Loading ML metrics for visualization..."):
        try:
            ml_metrics_df = load_ml_metrics_data(model_name)
            if ml_metrics_df.empty:
                st.write(f"No ML metrics data available for {model_name}.")
                return

            st.subheader(f"{model_name} ML Metrics Data (Interactive Table)")
            st.dataframe(
                ml_metrics_df,
                use_container_width=True,
                height=300
            )

            st.subheader(f"{model_name} Predicted Quantity Over Time")
            ml_metrics_df['forecast_timestamp'] = pd.to_datetime(ml_metrics_df['forecast_timestamp'])
            fig_line = px.line(
                ml_metrics_df.sort_values('forecast_timestamp'),
                x='forecast_timestamp',
                y='predicted_quantity',
                title=f"{model_name} Predicted Quantity Over Time",
                labels={'forecast_timestamp': 'Forecast Timestamp', 'predicted_quantity': 'Predicted Quantity'},
                hover_data=['predicted_quantity', 'forecast_timestamp']
            )
            fig_line.update_layout(
                xaxis_title="Forecast Timestamp",
                yaxis_title="Predicted Quantity",
                showlegend=True,
                dragmode='zoom',
                clickmode='event+select'
            )
            st.plotly_chart(fig_line, use_container_width=True)

            st.subheader(f"{model_name} Predicted vs Actual Quantity")
            # Filter rows where actual_quantity is not null
            scatter_df = ml_metrics_df[ml_metrics_df['actual_quantity'].notnull()]
            if scatter_df.empty:
                st.write("No actual quantities available to compare with predictions. This visualization will be available once actual data is collected for the forecasted dates.")
            else:
                fig_scatter = px.scatter(
                    scatter_df,
                    x='actual_quantity',
                    y='predicted_quantity',
                    color='category',
                    hover_data=['store_location', 'forecast_timestamp'],
                    title=f"{model_name} Predicted vs Actual Quantity",
                    labels={'actual_quantity': 'Actual Quantity', 'predicted_quantity': 'Predicted Quantity'}
                )
                fig_scatter.update_layout(
                    xaxis_title="Actual Quantity",
                    yaxis_title="Predicted Quantity",
                    showlegend=True,
                    dragmode='zoom',
                    clickmode='event+select'
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

        except Exception as e:
            st.error(f"Error loading ML metrics: {e}")

# View metrics (runs post_process_metrics.py and displays visualizations)
def view_metrics():
    log_file = 'D:/inventory_project/logs/process_inventory.log'
    if os.path.exists(log_file):
        with open(log_file, 'a') as f:
            f.write("\n--- Starting Post-Processing ---\n")
    
    with st.spinner("Running post-processing..."):
        try:
            result = subprocess.run(
                ["D:\\venv\\Scripts\\python.exe", "D:/inventory_project/notebooks/post_process_metrics.py"],
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode != 0:
                st.error(f"Post-processing failed with return code {result.returncode}: {result.stderr}")
                return
            st.success("Post-processing completed successfully.")
        except subprocess.TimeoutExpired:
            st.error("Post-processing timed out after 5 minutes.")
            return
        except Exception as e:
            st.error(f"Error during post-processing: {e}")
            return

    with st.spinner("Loading metrics for visualization..."):
        try:
            metrics_df = load_metrics_data()
            if metrics_df.empty:
                st.write("No metrics data available.")
                return

            st.subheader("Metrics Data (Interactive Table)")
            st.dataframe(
                metrics_df,
                use_container_width=True,
                height=300
            )

            st.subheader("Total Stock Value by Store Location")
            fig_bar = px.bar(
                metrics_df.groupby('store_location')['total_stock_value'].sum().reset_index(),
                x='store_location',
                y='total_stock_value',
                title="Total Stock Value by Store Location",
                labels={'store_location': 'Store Location', 'total_stock_value': 'Total Stock Value'},
                color='store_location',
                hover_data=['total_stock_value']
            )
            fig_bar.update_layout(
                xaxis_title="Store Location",
                yaxis_title="Total Stock Value",
                showlegend=True,
                dragmode='zoom',
                clickmode='event+select'
            )
            st.plotly_chart(fig_bar, use_container_width=True)

            if 'event_timestamp' in metrics_df.columns and 'turnover_rate' in metrics_df.columns:
                st.subheader("Turnover Rate Over Time")
                metrics_df['event_timestamp'] = pd.to_datetime(metrics_df['event_timestamp'])
                fig_line = px.line(
                    metrics_df.sort_values('event_timestamp'),
                    x='event_timestamp',
                    y='turnover_rate',
                    title="Turnover Rate Over Time",
                    labels={'event_timestamp': 'Event Timestamp', 'turnover_rate': 'Turnover Rate'},
                    hover_data=['turnover_rate', 'event_timestamp']
                )
                fig_line.update_layout(
                    xaxis_title="Event Timestamp",
                    yaxis_title="Turnover Rate",
                    showlegend=True,
                    dragmode='zoom',
                    clickmode='event+select'
                )
                st.plotly_chart(fig_line, use_container_width=True)

            st.subheader("Stock Value by Category and Store Location")
            pivot_table = metrics_df.pivot_table(
                values='total_stock_value',
                index='category',
                columns='store_location',
                aggfunc='sum',
                fill_value=0
            )
            fig_heatmap = px.imshow(
                pivot_table,
                labels=dict(x="Store Location", y="Category", color="Total Stock Value"),
                title="Stock Value Heatmap",
                color_continuous_scale='Viridis'
            )
            fig_heatmap.update_layout(
                dragmode='zoom',
                clickmode='event+select'
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)

            st.subheader("Total Quantity vs. Average Price by Category")
            fig_scatter = px.scatter(
                metrics_df,
                x='total_quantity',
                y='avg_price',
                color='category',
                size='total_stock_value',
                hover_data=['store_location', 'event_timestamp'],
                title="Total Quantity vs. Average Price by Category",
                labels={'total_quantity': 'Total Quantity', 'avg_price': 'Average Price'}
            )
            fig_scatter.update_layout(
                xaxis_title="Total Quantity",
                yaxis_title="Average Price",
                showlegend=True,
                dragmode='zoom',
                clickmode='event+select'
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

        except Exception as e:
            st.error(f"Error loading metrics: {e}")

# Function to display anomalies (used to avoid code duplication)
def display_anomalies(conn, table_name, title_prefix):
    st.subheader(f"{title_prefix} Numerical Anomalies (Quantity/Price)")
    if not table_exists(conn, table_name):
        st.write(f"No {table_name} table found in ml_metrics.db. Run anomaly detection to generate anomalies.")
        return pd.DataFrame()
    anomalies_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    if anomalies_df.empty:
        st.write(f"No numerical anomalies detected in {table_name}.")
    else:
        st.write(f"Found {len(anomalies_df)} numerical anomalies in {table_name}.")
        st.dataframe(anomalies_df, use_container_width=True)
        
        # Visualization: Anomalies by Store Location
        st.subheader(f"{title_prefix} Anomalies by Store Location")
        anomalies_by_location = anomalies_df.groupby('store_location').size().reset_index(name='count')
        fig_bar = px.bar(
            anomalies_by_location,
            x='store_location',
            y='count',
            title=f"Number of Anomalies by Store Location ({title_prefix})",
            labels={'store_location': 'Store Location', 'count': 'Number of Anomalies'},
            color='store_location'
        )
        fig_bar.update_layout(
            xaxis_title="Store Location",
            yaxis_title="Number of Anomalies",
            showlegend=True
        )
        st.plotly_chart(fig_bar, use_container_width=True)
        
        # Visualization: Anomalies by Category
        st.subheader(f"{title_prefix} Anomalies by Category")
        anomalies_by_category = anomalies_df.groupby('category').size().reset_index(name='count')
        fig_bar_category = px.bar(
            anomalies_by_category,
            x='category',
            y='count',
            title=f"Number of Anomalies by Category ({title_prefix})",
            labels={'category': 'Category', 'count': 'Number of Anomalies'},
            color='category'
        )
        fig_bar_category.update_layout(
            xaxis_title="Category",
            yaxis_title="Number of Anomalies",
            showlegend=True
        )
        st.plotly_chart(fig_bar_category, use_container_width=True)
    return anomalies_df

# Function to check if 'All' exists in store_location or category
def check_for_all(df):
    if df.empty:
        return False
    return (df['store_location'].str.contains('All', na=False).any() or 
            df['category'].str.contains('All', na=False).any())

# Function to migrate the dashboard_notifications table
def migrate_notifications_table(conn):
    """Migrate the dashboard_notifications table to include a category column if it doesn't exist."""
    if not table_exists(conn, 'dashboard_notifications'):
        print_and_log("dashboard_notifications table does not exist. Skipping migration.")
        return
    
    cursor = conn.cursor()
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

# Updated function to display anomaly detection results and notifications
def view_anomalies_and_notifications():
    st.header("Anomaly Detection and Notifications")
    
    # Path to scripts
    INTRODUCE_ANOMALIES_SCRIPT = "D:/inventory_project/notebooks/introduce_anomalies.py"
    INTRODUCE_ACTUAL_QUANTITY_SCRIPT = "D:/inventory_project/notebooks/introduce_actual_quantity.py"
    DETAILED_SCRIPT = "D:/inventory_project/notebooks/detailed.py"

    # Session state to track if anomalies have been introduced and anomaly detection runs
    if 'anomalies_introduced' not in st.session_state:
        st.session_state['anomalies_introduced'] = False
    if 'actual_quantities_introduced' not in st.session_state:
        st.session_state['actual_quantities_introduced'] = False
    if 'anomaly_detection_run' not in st.session_state:
        st.session_state['anomaly_detection_run'] = 0
    if 'selected_notification' not in st.session_state:
        st.session_state['selected_notification'] = None

    # Perform migration of dashboard_notifications table on section load
    with sqlite3.connect('D:/inventory_project/data/ml_metrics.db') as conn:
        migrate_notifications_table(conn)

    # Test Mode Options (only visible if Test Mode is enabled)
    test_mode = st.session_state.get('test_mode', False)
    if test_mode:
        st.subheader("Test Mode Options")
        st.markdown("Test mode activated! Insert random simulated values using the options below.")

        # Option 1: Introduce Anomalies
        if st.button("Introduce Anomalies"):
            # Check if the inventory table exists (dry run check)
            conn = get_db_connection('D:/inventory_project/data/inventory.db')
            if not table_exists(conn, 'inventory'):
                conn.close()
                st.error("Need dry run to add sim values.")
                return
            
            try:
                # Run the introduce_anomalies.py script
                result = subprocess.run(
                    ["D:\\venv\\Scripts\\python.exe", INTRODUCE_ANOMALIES_SCRIPT],
                    capture_output=True,
                    text=True,
                    timeout=300,
                    check=True
                )
                st.success("Anomalies have been introduced into the inventory data!")
                st.session_state['anomalies_introduced'] = True
                st.write("Check the logs at `D:/inventory_project/logs/introduce_anomalies.log` for details.")
                st.info("Anomalies introduced. Run Anomaly Detection to identify them, then view results below.")
            except subprocess.CalledProcessError as e:
                st.error(f"Failed to introduce anomalies: {e.stderr}")
            except Exception as e:
                st.error(f"An error occurred: {e}")
            finally:
                conn.close()

        # Option 2: Introduce Actual Quantities
        if st.button("Introduce Actual Quantities"):
            # Check if the ml_metrics table exists (dry run check)
            conn = get_db_connection('D:/inventory_project/data/ml_metrics.db')
            if not table_exists(conn, 'ml_metrics'):
                conn.close()
                st.error("Need dry run to add sim values.")
                return
            
            try:
                # Run the introduce_actual_quantity.py script
                result = subprocess.run(
                    ["D:\\venv\\Scripts\\python.exe", INTRODUCE_ACTUAL_QUANTITY_SCRIPT],
                    capture_output=True,
                    text=True,
                    timeout=300,
                    check=True
                )
                st.success("Actual quantities have been introduced into the ML metrics data!")
                st.session_state['actual_quantities_introduced'] = True
                st.write("Check the logs at `D:/inventory_project/logs/introduce_actual_quantity.log` for details.")
                st.info("Actual quantities introduced. Run ML Model for the affected model, then view ML Metrics to see the updates.")
            except subprocess.CalledProcessError as e:
                st.error(f"Failed to introduce actual quantities: {e.stderr}")
            except Exception as e:
                st.error(f"An error occurred: {e}")
            finally:
                conn.close()

    # Display messages if simulated data has been introduced
    if st.session_state['anomalies_introduced']:
        st.info("Anomalies have been introduced into the inventory data. Run anomaly detection to identify them.")
    if st.session_state['actual_quantities_introduced']:
        st.info("Actual quantities have been introduced into the ML metrics data. View ML metrics to see the updates.")

    # Run anomaly detection scripts
    st.subheader("Run Anomaly Detection")
    if st.button("Run Anomaly Detection"):
        with st.spinner("Running anomaly detection..."):
            try:
                # Run the numerical anomaly detection script (anomaly_detection.py)
                print_and_log("Running anomaly_detection.py...")
                result0 = subprocess.run(
                    ["D:\\venv\\Scripts\\python.exe", "D:/inventory_project/notebooks/anomaly_detection.py"],
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                if result0.returncode != 0:
                    raise Exception(f"anomaly_detection.py failed: {result0.stderr}")

                # Run existing anomaly detection scripts
                print_and_log("Running text_anomaly_detection.py...")
                result1 = subprocess.run(
                    ["D:\\venv\\Scripts\\python.exe", "D:/inventory_project/notebooks/text_anomaly_detection.py"],
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                if result1.returncode != 0:
                    raise Exception(f"text_anomaly_detection.py failed: {result1.stderr}")

                print_and_log("Running query.py...")
                result2 = subprocess.run(
                    ["D:\\venv\\Scripts\\python.exe", "D:/inventory_project/notebooks/query.py"],
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                if result2.returncode != 0:
                    raise Exception(f"query.py failed: {result2.stderr}")

                # Increment the anomaly detection run counter to trigger UI refresh
                st.session_state['anomaly_detection_run'] += 1
                st.success("Anomaly detection and notification generation completed successfully.")
            except Exception as e:
                print_and_log(f"Error during anomaly detection: {e}")
                st.error(f"Error during anomaly detection: {e}")
                return

    # Display anomalies and check for 'All'
    anomalies_df = pd.DataFrame()
    numerical_anomalies_df = pd.DataFrame()
    with st.container():
        with sqlite3.connect('D:/inventory_project/data/ml_metrics.db') as conn:
            anomalies_df = display_anomalies(conn, 'anomalies', "Pipeline")

    with st.container():
        with sqlite3.connect('D:/inventory_project/data/ml_metrics.db') as conn:
            numerical_anomalies_df = display_anomalies(conn, 'numerical_anomalies', "ML Model")

    # Check if 'All' exists and display "Want more details?" button
    has_all = check_for_all(anomalies_df) or check_for_all(numerical_anomalies_df)
    if has_all:
        st.subheader("Granularity Option")
        if st.button("Want more details?"):
            with st.spinner("Adding granular details to store_location and category..."):
                try:
                    result = subprocess.run(
                        ["D:\\venv\\Scripts\\python.exe", DETAILED_SCRIPT],
                        capture_output=True,
                        text=True,
                        timeout=300
                    )
                    if result.returncode != 0:
                        raise Exception(f"detailed.py failed: {result.stderr}")
                    st.session_state['anomaly_detection_run'] += 1
                    st.success("Granular details added successfully. Check the updated tables and notifications below.")
                except Exception as e:
                    st.error(f"Error adding granular details: {e}")

    # Display notifications with "Details" hyperlink
    with st.container():
        with sqlite3.connect('D:/inventory_project/data/ml_metrics.db') as conn:
            st.subheader("Notifications")
            if not table_exists(conn, 'dashboard_notifications'):
                st.write("No notifications table found in ml_metrics.db. Run anomaly detection to generate notifications.")
            else:
                notifications = pd.read_sql_query("SELECT * FROM dashboard_notifications", conn)
                if notifications.empty:
                    st.write("No notifications generated at this time.")
                else:
                    st.write(f"Found {len(notifications)} notifications.")
                    for idx, row in notifications.iterrows():
                        st.markdown(f"**Notification ({row['type']})**")
                        st.markdown(row['notification'])
                        # Add "Details" hyperlink
                        if st.button(f"Details", key=f"details_{idx}"):
                            st.session_state['selected_notification'] = row.to_dict()
                        
                        # Display detailed information if this notification is selected
                        if st.session_state['selected_notification'] and st.session_state['selected_notification']['notification'] == row['notification']:
                            st.markdown("**Detailed Information**")
                            try:
                                # Validate required columns
                                required_columns = ['store_location', 'category', 'event_timestamp']
                                missing_columns = [col for col in required_columns if col not in row]
                                if missing_columns:
                                    st.error(f"Missing required columns in notification: {missing_columns}")
                                    continue

                                # Fetch corresponding anomaly details
                                anomaly_table = 'numerical_anomalies' if row['type'] == 'numerical' else 'anomalies'
                                anomaly_df = pd.read_sql_query(
                                    f"SELECT * FROM {anomaly_table} WHERE store_location = ? AND category = ? AND {'forecast_timestamp' if row['type'] == 'numerical' else 'event_timestamp'} = ?",
                                    conn,
                                    params=(row['store_location'], row['category'], row['event_timestamp'])
                                )
                                if not anomaly_df.empty:
                                    anomaly_details = anomaly_df.iloc[0].to_dict()
                                    st.write("**Anomaly Details:**")
                                    for key, value in anomaly_details.items():
                                        st.write(f"{key}: {value}")
                                else:
                                    st.write("No additional details available for this notification.")
                            except Exception as e:
                                st.error(f"Error fetching anomaly details: {e}")

    # Display Test Mode Logs if Test Mode is enabled
    if test_mode:
        st.subheader("Test Mode Logs")
        # Display Introduce Anomalies Log
        anomalies_log = read_log_file('D:/inventory_project/logs/introduce_anomalies.log')
        if anomalies_log:
            st.subheader("Introduce Anomalies Log")
            with st.expander("View Introduce Anomalies Log", expanded=False):
                st.code(anomalies_log, language="plaintext", line_numbers=True)
        else:
            st.write("No Introduce Anomalies log available.")
        
        # Display Introduce Actual Quantities Log
        actual_quantities_log = read_log_file('D:/inventory_project/logs/introduce_actual_quantity.log')
        if actual_quantities_log:
            st.subheader("Introduce Actual Quantities Log")
            with st.expander("View Introduce Actual Quantities Log", expanded=False):
                st.code(actual_quantities_log, language="plaintext", line_numbers=True)
        else:
            st.write("No Introduce Actual Quantities log available.")
        
        # Display Detailed Granularity Log
        detailed_log = read_log_file('D:/inventory_project/logs/detailed.log')
        if detailed_log:
            st.subheader("Detailed Granularity Log")
            with st.expander("View Detailed Granularity Log", expanded=False):
                st.code(detailed_log, language="plaintext", line_numbers=True)
        else:
            st.write("No Detailed Granularity log available.")
    
    # User query section
    st.subheader("Ask About Anomalies")
    user_query = st.text_input("Enter your query (e.g., 'What should I do about an inventory issue?')")
    if user_query:
        response = handle_user_query(user_query)
        st.write("**Response:**")
        st.write(response)

# Sidebar navigation
st.sidebar.title("Inventory Management Dashboard")

# Test Mode Toggle
st.sidebar.subheader("Simulation Settings")
test_mode = st.sidebar.checkbox("Enable Test Mode", value=False)
st.session_state['test_mode'] = test_mode
if test_mode:
    st.sidebar.markdown("Simulate data for testing. Use options in Anomaly Detection section.")

# Main navigation sections
section = st.sidebar.radio(
    "Select Section",
    [
        "Ingest More Data",
        "Inventory Metrics",
        "Machine Learning Models",
        "Anomaly Detection and Notifications"
    ]
)

# Section: Ingest More Data
if section == "Ingest More Data":
    st.header("Ingest More Data")
    ingest_button = st.button("Ingest More Data")
    if ingest_button:
        with st.spinner("Ingesting data from Pulsar..."):
            try:
                success = ingest_data()
                if success:
                    st.success("Data ingestion completed successfully.")
            except Exception as e:
                st.error(f"Error during ingestion: {str(e)}")

# Section: Raw Inventory Data
elif section == "Inventory Metrics":
    st.header("Raw Inventory Data (Top 10 Rows)")
    raw_df = load_raw_data()
    if raw_df.empty:
        st.write("No inventory data available. Ingest data to populate the inventory table.")
    else:
        st.write(raw_df.head(10))

    st.header("Inventory Metrics")
    if 'metrics_processed' not in st.session_state:
        st.session_state.metrics_processed = False

    # Process Metrics Button
    if st.button("Process Metrics"):
        with st.spinner("Processing metrics..."):
            try:
                success = process_metrics()
                if success:
                    st.session_state.metrics_processed = True
                    st.success("Metrics processing completed. Click 'View Metrics' to see the results.")
            except Exception as e:
                st.error(f"Error during processing: {str(e)}")
                st.session_state.metrics_processed = False

    # View Metrics Button
    if st.session_state.metrics_processed:
        if st.button("View Metrics"):
            view_metrics()
    else:
        st.write("Click 'Process Metrics' to process the data first.")

# Section: Machine Learning Models
elif section == "Machine Learning Models":
    st.header("Machine Learning Models")
    if 'ml_processed' not in st.session_state:
        st.session_state.ml_processed = {}

    # Run ML Button
    st.subheader("Run ML Model")
    model_options = ["ARIMA", "XGBoost", "Isolation Forest", "Prophet"]
    selected_model = st.selectbox("Select ML Model", model_options)
    if st.button("Run ML"):
        with st.spinner(f"Running {selected_model} model..."):
            try:
                success = run_ml_model(selected_model)
                if success:
                    st.session_state.ml_processed[selected_model] = True
                    st.success(f"{selected_model} model processing completed. Click 'View ML Metrics' to see the results.")
            except Exception as e:
                st.error(f"Error during {selected_model} model processing: {str(e)}")
                st.session_state.ml_processed[selected_model] = False

    # View ML Metrics Button
    st.subheader("View ML Metrics")
    selected_view_model = st.selectbox("Select Model to View Metrics", model_options, key="view_ml_select")
    if selected_view_model in st.session_state.ml_processed and st.session_state.ml_processed.get(selected_view_model):
        if st.button("View ML Metrics"):
            view_ml_metrics(selected_view_model)
    else:
        st.write(f"Run the {selected_view_model} model first to view its metrics.")

# Section: Anomaly Detection and Notifications
elif section == "Anomaly Detection and Notifications":
    view_anomalies_and_notifications()