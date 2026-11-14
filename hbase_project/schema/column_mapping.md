# HBase Column Mapping – E-commerce Analytics

## 1. `ecom:browsing_events`

**Row key:** `user_id#reverse_timestamp#event_id`

| Column family | Qualifier | Source field | Representation |
|---|---|---|---|
| event | timestamp | `page_views.timestamp` | ISO 8601 string |
| event | page_type | `page_views.page_type` | String |
| event | view_duration | `page_views.view_duration` | Integer string |
| context | session_id | `session_id` | String |
| context | product_id | `page_views.product_id` | String, when available |
| context | category_id | `page_views.category_id` | String, when available |

Each page-view entry becomes an individual HBase row.

The reverse timestamp is calculated from the original event timestamp in milliseconds:

`reverse_timestamp = 9999999999999 - timestamp_ms`

This key structure groups browsing events by user and supports efficient retrieval of a user's activity in reverse chronological order. The event ID distinguishes multiple events belonging to the same session.

The `product_id` and `category_id` fields are stored only when available. For example, a home-page event may not have a product or category identifier.

**Browsing activity retrieval**

The table supports retrieving browsing activity for a specific user using a row-key prefix based on the user ID. The implemented retrieval script, `get_user_browsing_activity.py`, was tested with `user_000001`.

On November 6, 2026, HBase integration verification successfully retrieved 904 browsing events for `user_000001` using a row-key prefix scan without a result limit.

## 2. `ecom:product_metrics`

**Row key:** `product_id#date#hour`

Example:

`prod_000001#2026-08-02#13`

The hour is stored in the row key using two-digit formatting.

| Column family | Qualifier | Source | Representation |
|---|---|---|---|
| metrics | views | Product-detail browsing events | Integer string |
| metrics | cart_additions | Session cart snapshots | Integer string |
| metrics | purchases | Transaction line items | Integer string |
| metrics | quantity_sold | Transaction item quantities | Integer string |
| metrics | revenue | Transaction item subtotals | Decimal string |

The product ID, date, and hour are part of the row key. They are not duplicated as separate metric qualifiers.

Metrics are aggregated hourly for each product.

### 2.1 Aggregation sources and time assignment

- **Views:** Grouped by the date and hour of each product-detail page-view timestamp.
- **Cart additions:** Calculated from product quantities in session cart snapshots and grouped by the session end-time date and hour.
- **Purchases:** Transaction line items are grouped by the transaction timestamp date and hour.
- **Quantity sold:** Sum of purchased quantities grouped by product and transaction time bucket.
- **Revenue:** Sum of purchased product line-item subtotals grouped by product and transaction time bucket.

### 2.2 Loaded dataset

The full aggregation produced:

- 4,174,030 product-time records.
- 4,757 unique products.
- 91 dates, from August 2 to October 31, 2026.

A deterministic sample of 95,140 records was selected for HBase loading. It contains up to 20 evenly spaced records per product across its available timeline.

The sample preserves the original metric values and covers all 4,757 products.

The HBase table was successfully loaded and verified to contain exactly 95,140 rows.

## 3. Serialization Strategy

All HBase values are encoded as UTF-8 byte strings.

- Strings: UTF-8 text.
- Integers: Decimal strings.
- Decimal values: Decimal strings.
- Timestamps: ISO 8601 strings.
- Nested objects and arrays, where used in source processing: JSON strings.

Product metric values are stored as separate qualifiers in the `metrics` column family.

## 4. Metric Definitions

**Views:** Number of product-detail page-view events.

**Cart additions:** Sum of product quantities found in session cart snapshots. This is a snapshot-based measure, not a count of individual add actions.

**Purchases:** Number of transaction line items containing the product.

**Quantity sold:** Sum of purchased product quantities.

**Revenue:** Sum of purchased product line-item subtotals.

These definitions distinguish the number of purchase line items from the number of individual units sold.

## 5. Transaction Status Rules

The dataset contains three transaction statuses:

- `completed`
- `shipped`
- `delivered`

All three statuses are included in the product-metrics aggregation for purchases, quantity sold, and revenue because they represent valid purchase records in the dataset.

The original transaction status remains available in the MongoDB source data for further status-specific analysis.

## 6. Timestamp Format

Browsing and transaction timestamps use ISO 8601 format with microsecond precision.

Example:

`2026-08-09T02:56:49.876309`

For product metrics, the analytical date and hour are stored in the row key. HBase cell timestamps represent the time values were written to HBase, not the original event or transaction time.

## 7. Implemented HBase Tables

The HBase implementation for this project focuses on two tables:

1. **`ecom:browsing_events`** – Stores individual user browsing events in a time-series structure and supports retrieval of a specific user's browsing activity.
2. **`ecom:product_metrics`** – Stores hourly product performance metrics, including views, cart additions, purchases, quantity sold, and revenue.

Transaction records are maintained in MongoDB and used as a source for product-metrics aggregation. They are not duplicated in a separate HBase transactions table.

This two-table implementation focuses on the required HBase deliverables while using the strengths of each database system.
