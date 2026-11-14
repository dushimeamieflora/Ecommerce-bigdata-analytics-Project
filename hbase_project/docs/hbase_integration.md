# HBase Integration Documentation
## E-commerce Big Data Analytics Project

### 1. Overview

Apache HBase was integrated into the e-commerce project to store and retrieve large volumes of browsing activity and product performance data.

The project uses Dockerized HBase 2.6.7 and Python HappyBase through the HBase Thrift service.

### 2. Environment

| Component | Configuration |
|---|---|
| HBase | 2.6.7 |
| Deployment | Docker standalone |
| Docker container | ecommerce-hbase |
| Python library | HappyBase |
| Host | localhost |
| Thrift port | 9090 |
| Namespace | ecom |

### 3. HBase Tables

#### 3.1 Browsing Events

Table: ecom:browsing_events

Purpose: Stores customer browsing activity, including page views, event types, timestamps, session IDs and view durations.

Row key: user_id#reverse_timestamp#event_id

Column families:
- event: page type, timestamp and view duration.
- context: session ID and browsing context.

Historically verified row count (November 6, 2026): 10,301,209.

#### 3.2 Product Metrics

Table: ecom:product_metrics

Purpose: Stores hourly product performance metrics aggregated from browsing activity and transaction data.

Row key: product_id#date#hour

Column family: metrics

Qualifiers:
- views
- cart_additions
- purchases
- quantity_sold
- revenue

Historically verified row count (November 6, 2026): 95,140.

Important: This table contains a deterministic, product-balanced temporal sample of the full product metrics aggregation. It is intended for HBase storage and query demonstrations, not for calculating complete business totals.

### 4. Data Integration Process

1. Created the ecom namespace and HBase tables.
2. Loaded browsing events from JSONL session files.
3. Aggregated product performance metrics from browsing activity and transaction data.
4. Generated a deterministic sample containing up to 20 evenly spaced hourly records per product.
5. Loaded the sample into HBase.
6. Executed HBase Shell queries.
7. Verified connectivity and data retrieval using Python HappyBase.

### 5. Verification Results

Verification script: scripts/verify_hbase_integration.py

All Python verification checks passed during the November 6, 2026 verification run:

| Verification | Result |
|---|---|
| HBase connection | PASS |
| Browsing events table exists | PASS |
| Product metrics table exists | PASS |
| Retrieve all events for user_000001 | PASS |
| Retrieve a specific browsing event | PASS |
| Retrieve product metric records | PASS |
| Verify five metric qualifiers | PASS |

The selected user user_000001 had 904 browsing events.

A specific cart event was successfully retrieved with its session ID, timestamp and view duration.

### 6. Query Verification

HBase Shell queries successfully tested:

- Retrieving the first five browsing events for a user.
- Retrieving all browsing events for a user.
- Retrieving a specific browsing event.
- Retrieving product metrics by product prefix.
- Retrieving product metrics by date.
- Retrieving a specific product-time record.
- Selecting individual metric columns.
- Counting records in both tables.

### 7. Technical Outcomes

The integration demonstrates that HBase can:

- Store large-scale browsing activity.
- Organize records using row keys and column families.
- Retrieve user-specific browsing information.
- Retrieve individual event records.
- Store hourly product performance metrics.
- Support targeted browsing and product queries.

Python HappyBase successfully connected to HBase through the Thrift service.

### 8. Limitations and Considerations

- The product metrics table contains a deterministic sample of 95,140 records, not the complete 4,174,030-record aggregation. The sample is not random and should not be assumed statistically representative.
- The full product metrics CSV is retained separately for comprehensive analysis.
- The total table counts were verified using HBase Shell. The Python verification script records them as reference counts rather than recounting the complete tables.
- The browsing event count represents the successfully loaded source page-view records.

### 9. Implemented Spark Integration

The project has implemented batch-based integration across MongoDB, HBase, and Apache Spark.

The workflow exports MongoDB collections and HBase tables into JSON Lines files. Apache Spark reads these exports, performs cross-system joins and analytical transformations, and writes integrated Parquet and CSV results.

Relevant implementation scripts include:

- `integration/end_to_end/02_export_mongodb_collections.py`
- `integration/hbase_spark/01_export_hbase_tables.py`
- `integration/end_to_end/03_mongodb_hbase_spark_analysis.py`
- `integration/end_to_end/04_export_integrated_results_to_csv.py`
- `integration/end_to_end/05_validate_integrated_results.py`
- `integration/end_to_end/06_create_integrated_visualizations.py`

This is a batch export-and-process architecture, not a live federated query engine.
