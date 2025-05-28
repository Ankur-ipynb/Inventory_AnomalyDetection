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
+-------------------+
|  Data Ingestion   |  (ingest.py: Pulsar -> SQLite)
|  (Raw Data)       |
+-------------------+
          ^
          |
+-------------------+
|  Storage Layer    |  (inventory.db, inventory_metrics.db, ml_metrics.db)
|  (SQLite DBs)     |
+-------------------+
