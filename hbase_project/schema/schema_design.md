# HBase Schema Design – E-commerce Analytics

## 1. Overview

The HBase implementation uses a wide-column data model designed to support time-series browsing analysis, product performance monitoring, and user activity retrieval.

The schema is designed around the expected analytical queries rather than directly copying the original JSON structure.

Two tables are implemented and validated in the `ecom` namespace:

- `ecom:browsing_events`
- `ecom:product_metrics`

The browsing-events table stores individual user browsing events, while the product-metrics table stores hourly aggregated product performance data.

Transaction records remain in MongoDB. They are used as a source for calculating product performance metrics rather than being duplicated in a separate HBase transactions table.

## 2. Table Design

| Table | Row key | Column families | Purpose |
|---|---|---|---|
| `ecom:browsing_events` | `user_id#reverse_timestamp#event_id` | `event`, `context` | Store individual browsing events and retrieve a user's activity |
| `ecom:product_metrics` | `product_id#date#hour` | `metrics` | Store product performance metrics over time |

## 3. Table Design Justification

### 3.1 Browsing Events

The `ecom:browsing_events` table stores individual page-view events extracted from the `page_views` array in the original session JSONL files.

Its composite row key follows this structure:

`user_id#reverse_timestamp#event_id`

The reverse timestamp is calculated by subtracting the event timestamp in milliseconds from a large fixed value:

`reverse_timestamp = 9999999999999 - timestamp_ms`

This design groups records belonging to the same user and orders their events from newest to oldest when scanned by row-key prefix.

The table uses two column families:

**`event`**
- `page_type`
- `timestamp`
- `view_duration`

**`context`**
- `session_id`
- `product_id` (when available)
- `category_id` (when available)

This separation distinguishes the event's descriptive information from its associated session and product context.

A prefix-based scan using a specific user ID allows efficient retrieval of that user's browsing activity without scanning the entire table.

The implementation was tested by retrieving browsing records for `user_000001` through a Python HappyBase client.

On November 6, 2026, HBase integration verification successfully retrieved 904 browsing events for `user_000001` using a row-key prefix scan without a result limit.

### 3.2 Product Metrics

The `ecom:product_metrics` table stores aggregated product performance metrics over time.

Its row key follows this structure:

`product_id#date#hour`

For example:

`prod_000001#2026-08-02#13`

The hour is represented using two digits to maintain consistent row-key formatting.

The table uses one column family:

**`metrics`**

| Qualifier | Description |
|---|---|
| `views` | Number of product-detail page views |
| `cart_additions` | Sum of product quantities found in session cart snapshots |
| `purchases` | Number of transaction line items containing the product |
| `quantity_sold` | Total quantity purchased |
| `revenue` | Total revenue from the product's transaction line-item subtotals |

This design supports product-level performance retrieval and comparisons across dates and hours.

The product metrics were aggregated from 20 non-empty session files and five non-empty transaction files.

The full aggregation produced:

- 4,174,030 product-time records
- 4,757 unique products
- 91 dates, from August 2 to October 31, 2026

To keep the HBase demonstration manageable within the available local resources, a deterministic sample of 95,140 records was loaded.

The sample includes up to 20 evenly spaced records per product across its available timeline. All selected metric values were preserved without modification.

The original full aggregation CSV was retained separately.

The HBase table was successfully loaded and verified to contain exactly 95,140 rows.

## 4. Denormalization

HBase is a distributed wide-column database rather than a relational database. Therefore, information may be duplicated across tables to support different access patterns.

For example, user IDs appear in browsing events, while product IDs appear in browsing events and product metrics. Product transaction information is retained in MongoDB and contributes to the product metrics aggregation.

This organization allows records to be structured according to their main access patterns and reduces the need for relational joins during HBase retrieval.

## 5. Time-Series Design

Browsing events retain their original event timestamps in ISO 8601 format.

The browsing-events row key uses reverse timestamps to place recent user activity before older activity during a prefix scan.

Product performance metrics are aggregated into hourly time buckets. The product ID, date, and hour are included in the row key to support time-oriented product retrieval.

HBase cell timestamps represent the time a value was written to HBase. The original analytical date and hour are preserved in the product-metrics row key.

## 6. Data Serialization

The implementation uses the following serialization conventions:

- Strings: UTF-8.
- Integer metrics: decimal strings.
- Decimal metrics: decimal strings.
- Timestamps: ISO 8601 strings.
- Nested objects and arrays: JSON strings where applicable.
- Product metric values: integer or decimal strings, depending on the metric.

The product-metrics table stores its five metric values as separate columns within the `metrics` family.

## 7. Expected Analytical Queries

The HBase design supports the following queries:

1. Retrieve browsing events for a specific user using a row-key prefix.
2. Retrieve a user's recent browsing activity in reverse chronological order.
3. Retrieve product metrics for a specific product.
4. Retrieve product performance for a specific date or hour.
5. Compare product views, cart additions, purchases, quantities sold, and revenue over time.

The browsing-event retrieval and product-metrics queries were implemented and tested using HBase Shell and Python HappyBase.

## 8. Implemented HBase Tables

The final implementation focuses on two HBase tables:

- `ecom:browsing_events` for user browsing activity.
- `ecom:product_metrics` for hourly product performance metrics.

Separate HBase tables for user sessions and transactions were not included in the final implementation because their required analytical information is already supported by the retained tables and MongoDB.

This two-table design keeps the HBase implementation focused on the required access patterns while avoiding unnecessary duplication of transaction data.
