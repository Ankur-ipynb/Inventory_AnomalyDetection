Inventory Anomaly Detection & Management Dashboard
  
The Inventory Management Dashboard is a web-based application designed to monitor inventory metrics, detect anomalies, and provide predictive insights using machine learning models. Built with Streamlit, it integrates real-time data ingestion, anomaly detection pipelines, and interactive visualizations to help inventory managers identify and resolve issues efficiently.
Features

Real-Time Data Ingestion: Ingest inventory data from Apache Pulsar/Fallback DB.
Inventory Metrics: Visualize stock value, turnover rates, and more with interactive charts.
Anomaly Detection: Identify unusual patterns in inventory data using pipeline-based and ML-based methods.
Machine Learning Predictions: Leverage models like ARIMA, XGBoost, Isolation Forest, and Prophet for forecasting.
Notifications System: Receive alerts for detected anomalies with detailed insights.
Test Mode: Simulate anomalies and actual quantities for testing purposes.
Data Generation: Generate synthetic inventory and text data for testing and development.

Project Structure
inventory_project/
├── data/
│   ├── inventory.db           # Raw inventory data
│   ├── inventory_metrics.db   # Processed inventory metrics
│   └── ml_metrics.db          # ML model predictions and anomalies
├── design.txt                 # Project design notes or documentation
├── libs/
│   └── sqlite-jdbc-3.49.1.0.jar  # SQLite JDBC driver for Spark
├── logs/
│   ├── anomaly_detection.log      # Logs for numerical anomaly detection
│   ├── app.log                    # Dashboard logs (likely a duplicate of dashboard.log)
│   ├── arima_model.log            # Logs for ARIMA model execution
│   ├── detailed.log               # Logs for detailed granularity updates
│   ├── generate_text_data.log     # Logs for generating text data
│   ├── ingest_data.log            # Logs for data ingestion
│   ├── introduce_actual_quantity.log  # Logs for introducing actual quantities in test mode
│   ├── introduce_anomalies.log    # Logs for introducing anomalies in test mode
│   ├── isolationforest_model.log  # Logs for Isolation Forest model execution
│   ├── post_process_metrics.log   # Logs for post-processing metrics
│   ├── process_inventory.log      # Logs for inventory metrics processing
│   ├── prophet_model.log          # Logs for Prophet model execution
│   ├── query.log                  # Logs for notification generation
│   ├── recruiter_attention.log    # Logs for a script likely used for generating recruiter-focused data or reports
│   ├── text_anomaly_detection.log # Logs for text-based anomaly detection
│   ├── view_ml_metrics.log        # Logs for viewing ML metrics
│   └── xgboost_model.log          # Logs for XGBoost model execution
├── notebooks/
│   ├── .ipynb_checkpoints/        # Jupyter notebook checkpoints (auto-generated)
│   │   ├── anomaly_detection-checkpoint.py
│   │   ├── app-checkpoint.py
│   ├── ...                       # Other notebook checkpoints (numerous files)
│   ├── __pycache__/               # Python cache files
│   │   └── query.cpython-39.pyc
│   ├── anomaly_detection.py       # Detects numerical anomalies in inventory data
│   ├── app.py                    # Main Streamlit dashboard
│   ├── arima_model.py            # ARIMA model for forecasting
│   ├── detailed.py               # Replaces 'All' with granular store_location and category values
│   ├── generate_data.py          # Generates synthetic inventory data
│   ├── generate_text_data.py     # Generates synthetic text data for testing
│   ├── ingest.py                 # Ingests data from Apache Pulsar/ fallback DB
│   ├── introduce_actual_quantity.py  # Simulates actual quantities in test mode
│   ├── introduce_anomaly.py      # Simulates anomalies in test mode (likely a duplicate of introduce_anomalies.py)
│   ├── isolation forest_model.py # Isolation Forest model for anomaly detection
│   ├── migrate_notifications.py  # Migrates notifications table to add category column
│   ├── post_process_metrics.py   # Post-processes metrics for visualization
│   ├── process_inventory.py      # Processes inventory data using Spark
│   ├── prophet_model.py          # Prophet model for forecasting
│   ├── query.py                  # Generates notifications from anomalies
│   ├── text_anomaly_detection.py # Detects text-based anomalies
│   ├── view_ml_metrics.py        # Visualizes ML model metrics
│   └── xgboost_model.py          # XGBoost model for forecasting
├── spark-events/                 # Spark event logs
│   ├── local-1747900235094
│   ├── local-1747900237436
│   ├── ...                       # Other Spark event logs (numerous files)
│   └── local-1748421990144
├── spark-tmp/                    # Temporary directory for Spark
│   └── spark-a4719fde-59be-4ffc-a0e6-afb953a54808/
│       └── userFiles-01c12c5f-0f33-4a0c-be1b-def7fc46eb6f/
│           └── sqlite-jdbc-3.49.1.0.jar  # Copy of SQLite JDBC driver used by Spark
├── spark-warehouse/              # Spark SQL warehouse
└── README.md                     # Project documentation

Prerequisites

Python 3.9+
Apache Spark (for processing and ML model execution)
Hadoop (for Spark compatibility on Windows)
Apache Pulsar (for data ingestion)
SQLite (for database storage)

1. Set Up a Virtual Environment
python -m venv venv
venv\Scripts\activate  # On Windows
# source venv/bin/activate  # On macOS/Linux

2. Install Dependencies
Install the required Python packages:
pip install pandas streamlit sqlite3 pyspark plotly pulsar-client

3. Set Up Hadoop and Spark

Download Hadoop:

Download Hadoop binaries compatible with your system (e.g., Hadoop 3.2.0).
Extract to D:\hadoop.
Set the environment variable:set HADOOP_HOME=D:\hadoop
set PATH=%PATH%;%HADOOP_HOME%\bin


Ensure winutils.exe is in D:\hadoop\bin. Download it from a trusted source if missing.
(Optional) Download hadoop.dll and place it in D:\hadoop\bin to suppress NativeCodeLoader warnings.


Download Spark:

Download Apache Spark (e.g., Spark 3.3.0) pre-built for Hadoop.
Extract to a directory (e.g., D:\spark).
Set the environment variable:set SPARK_HOME=D:\spark
set PATH=%PATH%;%SPARK_HOME%\bin




Download SQLite JDBC Driver:

Download sqlite-jdbc-3.49.1.0.jar from Maven Repository.
Place it in inventory_project/libs/.



4. Configure Environment Variables
Set the following environment variables (update paths as needed):
set HADOOP_HOME=D:\hadoop
set PATH=%PATH%;D:\hadoop\bin
set SPARK_CLASSPATH=D:\inventory_project\libs\sqlite-jdbc-3.49.1.0.jar
set PYSPARK_PYTHON=D:\inventory_project\venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=D:\inventory_project\venv\Scripts\python.exe

5. Initialize Databases
The project uses three SQLite databases:

inventory.db: Stores raw inventory data.
inventory_metrics.db: Stores processed metrics.
ml_metrics.db: Stores ML metrics, Anomalies, Dashboard Notifications, Numerical Anomalies

These databases are created automatically when scripts run, but ensure the data/ directory exists:
mkdir data

Usage
1. Generate Synthetic Data (Optional)
To populate the databases with synthetic data for testing:

Generate Inventory Data:python notebooks/generate_data.py - functionality inclusion in V2
Generate Text Data:python notebooks/generate_text_data.py - functionality inclusion in V2

2. Run the Dashboard
Start the Streamlit dashboard:
streamlit run notebooks/app.py


Open your browser and navigate to http://localhost:8501.

3. Dashboard Sections

Ingest More Data:
Click "Ingest More Data" to fetch new inventory data from Pulsar.


Inventory Metrics:
Click "Process Metrics" to compute inventory metrics.
Click "View Metrics" to see visualizations (e.g., stock value by store, turnover rate over time).


Machine Learning Models:
Select a model (ARIMA, XGBoost, Isolation Forest, Prophet) and click "Run ML" to generate predictions.
Click "View ML Metrics" to visualize predicted vs. actual quantities.


Anomaly Detection and Notifications:
Click "Run Anomaly Detection" to detect anomalies in inventory data.
View notifications and click "Details" for more information.
(Optional) Click "Want more details?" to replace generic values (e.g., "All") with granular ones.



4. Test Mode

Enable "Test Mode" in the sidebar to simulate data:
Introduce Anomalies: Add random anomalies to the inventory data.
Introduce Actual Quantities: Add actual quantities to ML metrics for comparison.



Database Schemas
inventory.db

Table: inventory
Columns: product_id, category, store_location, quantity, price, event_timestamp



inventory_metrics.db

Table: metrics
Columns: store_location, category, total_quantity, avg_price, total_stock_value, turnover_rate, event_timestamp



ml_metrics.db

Table: ml_metrics
Columns: model_name, store_location, category, predicted_quantity, actual_quantity, forecast_timestamp


Table: anomalies
Columns: description, method, store_location, category, event_timestamp


Table: numerical_anomalies
Columns: model_name, anomaly_type, anomaly_description, store_location, category, forecast_timestamp


Table: dashboard_notifications
Columns: type, notification, store_location, event_timestamp, product_id, category



Additional Notes

Design Documentation:

design.txt contains project design notes or architecture details.

Logs:

All logs are stored in the logs/ directory. Check specific log files for debugging (e.g., app.log, query.log).

Known Issues

Hadoop Warning:

If hadoop.dll is missing in D:\hadoop\bin, you may see a NativeCodeLoader warning. This doesn’t affect functionality but can be resolved by downloading the correct hadoop.dll.

Duplicate Files:
Clean up necessary post large data test runs to reduce latency.


Please follow Python PEP 8 style guidelines and include tests where applicable.
License
This project is licensed under the MIT License - see the LICENSE file for details.
Contact
For questions or issues, please open an issue on the GitHub repository or contact the maintainers at ankur3103verma@gmail.com.
