Inventory Management Dashboard - Design Document
Last Updated: May 28, 2025, 03:49 PM IST
Project Overview
The Inventory Management Dashboard is a web-based application designed to monitor and manage inventory data for multiple store locations. Its primary goals are:

Ingest real-time inventory data from Apache Pulsar.
Process inventory data and perform near real-time processing.
Detect anomalies in inventory data using both rule-based and ML-based methods.
Provide predictive insights using ML models (ARIMA, XGBoost, Prophet, Isolation Forest).
Present actionable insights through an interactive dashboard with visualizations and notifications.

The system is built to support inventory managers by helping them to identify discrepancies, predict stock levels, and manage inventory more effectively.
Component Descriptions
Data Generation Components

generate_data.py: Generates synthetic inventory data for testing.
generate_text_data.py: Generates synthetic text data for text-based anomaly detection.

Test Mode Components

introduce_anomaly.py / introduce_anomalies.py: Simulates anomalies in inventory data.
introduce_actual_quantity.py: Simulates actual quantities for ML metrics comparison.

Pipeline Components

migrate_notifications.py: Adds a category column to the dashboard_notifications table (now embedded in detailed.py).

Supporting Files

design.txt: The original design document (replaced by this modular set of markdown files).
SOA.pptx: Design presentation for the project.

Database Design
inventory.db

Table: inventory
product_id (TEXT)**: Unique identifier for the product.
category (TEXT): Product category (e.g., Electronics, Clothing).
store_location (TEXT): Store location (e.g., Store_A, Store_B).
quantity (INTEGER): Stock quantity.
price (FLOAT): Price per unit.
event_timestamp (TEXT): Timestamp of the event.



inventory_metrics.db

Table: metrics
store_location (TEXT): Store location identifier.
category (TEXT): Product category.
total_quantity (INTEGER): Total quantity across products.
avg_price (FLOAT): Average price of products.
total_stock_value (FLOAT): Total stock value (quantity * price).
turnover_rate (FLOAT): Inventory turnover rate.
event_timestamp (TEXT): Timestamp of the event.



ML Models

Table: ml_metrics

model_name (TEXT): Name of the ML model (e.g., ARIMA, XGBoost).
store_location (TEXT): Store location.
category (TEXT): Product category.
predicted_quantity (FLOAT): Predicted stock quantity.
actual_quantity (FLOAT): Actual stock quantity (nullable).
forecast_timestamp (TEXT): Timestamp of the prediction.


Table: anomalies

description (TEXT): Description of the anomaly.
method (TEXT): Detection method used (e.g., rule-based, statistical).
store_location (TEXT): Store location identifier.
category (TEXT): Product category.
event_timestamp (TEXT): Timestamp of the event.


Table: numerical_quantity

model_name (TEXT): ML model that detected the anomaly.
anomaly_type (TEXT): Type of anomaly (e.g., quantity discrepancy).
anomaly_description (TEXT): Detailed description of the anomaly.
store_location (TEXT): Store location identifier.
category (TEXT): Product category.
forecast_timestamp (TEXT): Timestamp of the forecast.


Table: dashboard_notifications

type (TEXT): Notification type (e.g., pipeline, numerical).
notification (TEXT): Notification message).
store_location (TEXT): Store location.
event_timestamp (TEXT): Timestampu of the event.
product_id (TEXT): Product identifier (often placeholder).
category (TEXT): Product category.



Scalability and Future Improvements
Scalability

The system uses Apache Spark for distributed processing, making it scalable for large datasets.
SQLite databases may become a bottleneck for very large datasets; consider migrating to a distributed database like PostgreSQL or Apache Cassandra.

Future Improvements (v2)

Add user authentication to secure the dashboard.
Enhance anomaly detection with deep learning models (e.g., LSTM for time-series anomalies).
Optimize Spark jobs for faster processing (e.g., caching, partitioning).
Add more interactive visualizations (e.g., time-series forecasting charts).
Automate testing with unit tests for key components.
Set up CI/CD pipelines using GitHub Actions for automated testing and deployment.
Implement true real-time feature analysis by using Spark Streaming to process Pulsar messages continuously.
Improve real-time ingestion from Pulsar in ingest.py to align with the original design intent.

