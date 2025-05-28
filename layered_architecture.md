Layered Architecture
Last Updated: May 28, 2025, 03:49 PM IST
System Architecture
The system follows a layered architecture with the following components:
Data Ingestion Layer

Source: Apache Pulsar (near real-time data publishing and consumption).
Scripts:
ingest.py: Generates synthetic inventory data and publishes it to Pulsar.
process_inventory.py: Consumes data from Pulsar (or SQLite as a fallback) for near real-time batch processing.



Data Processing Layer

Tool: Apache Spark
Scripts:
process_inventory.py: Processes inventory data to compute metrics (e.g., total stock value, stock value by category).
post_process_metrics.py: Prepares processed data for visualization.
ML Scripts (arima_model.py, xgboost_model.py, isolation_forest_model.py, prophet_model.py): Train models and generate predictions.



Anomaly Detection Layer

Scripts:
anomaly_detection.py: Detects numerical anomalies (e.g., quantity/price discrepancies).
text_anomaly_detection.py: Detects text-based anomalies in inventory descriptions.
query.py: Generates notifications based on detected anomalies.
detailed.py: Enhances granularity by replacing generic values (e.g., "All") with specific store locations and categories.



Presentation Layer

Tool: Streamlit
Script: app.py
Provides an interactive dashboard with sections for data ingestion, metrics visualization, ML model predictions, and anomaly detection.
Features test mode for simulating anomalies and actual quantities.



Storage Layer

Databases:
inventory.db: Stores raw inventory data.
inventory_metrics.db: Stores processed metrics.
ml_metrics.db: Stores ML predictions, anomalies, and notifications.



System Architecture Diagram (ASCII)
Below is an ASCII representation of the system architecture, showing the layered structure:
+-------------------+
|  Presentation     |  (app.py: Streamlit Dashboard)
|  (Visualizations) |
+-------------------+
          ^
          |
+-------------------+
|  Anomaly Detection|  (anomaly_detection.py, text_anomaly_detection.py, query.py, detailed.py)
|  (Notifications)  |
+-------------------+
          ^
          |
+-------------------+
|  Data Processing  |  (process_inventory.py, post_process_metrics.py, ML models)
|  (Spark, ML)      |
+-------------------+
          ^
          |
          |
+-------------------+
|  Data Ingestion   |  (ingest.py: Publishes to Pulsar; process_inventory.py: Consumes from Pulsar, near real-time batch processing)
|  (Raw Data)       |
+-------------------+
          ^
          |
          |
+-------------------+
|  Storage Layer    |  (inventory.db, inventory_metrics.db, ml_metrics.db)
|  (SQLite DBs)     |
+-------------------+

