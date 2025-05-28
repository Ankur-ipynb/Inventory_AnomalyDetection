Data Flow Diagram (ASCII)
Below is an ASCII representation of the data flow through the system:
[ingest.py] --> [inventory.db] --> [Pulsar: near real-time data publishing]
                       |                     |
                       v                     v
              [process_inventory.py] <-------|  --> [inventory_metrics.db]
                       |
                       v
              [ML Models] --> [ml_metrics.db]
                       |
                       v
              [anomaly_detection.py] --> [ml_metrics.db: anomalies]
                       |
                       v
              [query.py] [ml_metrics.db] --> [ml_metrics.db: notifications]
                       |
                       v
              [detailed.py] --> [ml_metrics.db: refined notifications]
                       |
                       v
[app.py] <-------------| [Display: Metrics, Predictions, Predictions, Anomalies, Notifications]

