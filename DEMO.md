Demo Guide for Inventory Management Dashboard
Last Updated: May 28, 2025, 03:58 PM IST
This guide walks you through running a demo of the Inventory Management Dashboard, showcasing its features like real-time data ingestion, metrics visualization, anomaly detection, and ML predictions.
Prerequisites

Python 3.8+ installed.
Apache Spark and Pulsar set up (see README.md for setup instructions).
Hadoop winutils.exe placed at D:\hadoop\bin (for Windows users).
Dependencies installed:pip install -r requirements.txt



Running the Demo

Generate and Ingest Data:

Run ingest.py to generate synthetic inventory data and publish it to Pulsar:python notebooks/ingest.py


This will populate data/inventory.db and send data to the Pulsar topic persistent://public/default/inventory-topic.


Process the Data:

Run process_inventory.py to consume data from Pulsar, compute metrics using Spark, and store results in data/inventory_metrics.db:python notebooks/process_inventory.py




Run ML Models (Optional):

To generate predictions, run the ML model scripts:python notebooks/arima_model.py
python notebooks/xgboost_model.py
python notebooks/isolation_forest_model.py
python notebooks/prophet_model.py


Results will be stored in data/ml_metrics.db.


Detect Anomalies (Optional):

Run anomaly detection scripts to identify discrepancies:python notebooks/anomaly_detection.py
python notebooks/text_anomaly_detection.py


Notifications will be generated and stored in data/ml_metrics.db.


Launch the Dashboard: Requires entire set of code to be stored in local for local test

Start the Streamlit dashboard:streamlit run app.py


Open the provided URL (e.g., http://localhost:8501) in your browser. 



Demo Features to Explore

Data Ingestion Section: Use the "Ingest More Data" button to generate and ingest new data.
Metrics Visualization: View total stock value, average price, and stock value by category across store locations.
ML Predictions: See predicted stock quantities from ARIMA, XGBoost, and Prophet models.
Anomaly Detection: Check for numerical and text-based anomalies with detailed notifications.
Test Mode: Simulate anomalies and actual quantities to test the system’s robustness.

Notes

Ensure Pulsar is running at pulsar://172.27.235.96:6650. If Pulsar is unavailable, the system will fall back to SQLite (data/inventory.db).
Logs are available in logs/ for debugging (ingest.log, process_inventory.log).
For a full project overview, see README.md and the design documentation in layered_architecture.md, flow_diagram.md, and design.md.

Enjoy the demo!
