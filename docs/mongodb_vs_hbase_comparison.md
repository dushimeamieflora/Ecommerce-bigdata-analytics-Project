# MongoDB vs HBase: Technology Comparison

## 1. Introduction

This project uses MongoDB and Apache HBase as complementary NoSQL databases for storing and retrieving e-commerce data. Both technologies support large-scale data storage, but they use different data models and are suited to different access patterns.

MongoDB stores the structured e-commerce entities, while HBase stores high-volume browsing events and pre-aggregated product performance metrics.

## 2. MongoDB Implementation

MongoDB is a document-oriented NoSQL database that stores data in flexible BSON documents.

In this project, MongoDB stores four collections:

| Collection | Documents | Purpose |
|---|---:|---|
| users | 10,000 | Customer profiles |
| categories | 25 | Product categories |
| products | 5,000 | Product catalog |
| transactions | 500,000 | Customer purchase records |

The transaction documents contain embedded line items, allowing each transaction and its purchased products to be stored together.

### Why MongoDB was selected

- Its document model supports flexible e-commerce records.
- Embedded line items keep transaction details together.
- Aggregation pipelines support analytical operations such as grouping, summing, sorting, and joining collections.
- It supports indexes for frequently used queries.
- It provides convenient access to individual customer, product, and transaction records.

### Implemented analytical queries

Three aggregation pipelines were implemented:

1. **Revenue by category:** Calculates gross item revenue and quantities by product category.
2. **Monthly revenue by category:** Calculates monthly gross item revenue, quantities, and category-specific transaction counts.
3. **Top-selling products:** Returns the top 10 products ranked by quantity sold, with gross item revenue.

The pipelines operate on transaction documents without an initial sampling stage. Revenue is calculated from `items.subtotal`, before transaction-level discounts. A transaction containing products from multiple categories can contribute to multiple category-specific transaction counts.
## 3. HBase Implementation

Apache HBase is a distributed, column-family-oriented NoSQL database designed for large datasets and scalable random access.

This project uses two HBase tables:

| HBase table | Row count | Purpose |
|---|---:|---|
| ecom:browsing_events | Loaded from browsing-event data | Stores individual user browsing events |
| ecom:product_metrics | 95,140 (historically verified) | Stores a deterministic sample of hourly product performance metrics |

### Why HBase was selected

- It supports large volumes of time-oriented event data.
- Its row-key design supports efficient retrieval by user or product and time.
- Column families organize related event and metric attributes.
- It supports horizontal scalability across distributed storage.
- It is suitable for workloads involving frequent writes and targeted lookups.

### Row-key design

**Browsing events**

The row key is:

`user_id#reverse_timestamp#event_id`

This design groups events by user and uses a reversed timestamp to support retrieval of recent events first when scanning a user's rows.

**Product metrics**

The row key is:

`product_id#date#hour`

This design organizes hourly metrics by product and time, supporting targeted retrieval of a product's performance for a particular hour.

### Implemented queries

- Retrieve browsing events for a specific user.
- Retrieve the latest browsing events for a user.
- Retrieve an individual browsing event using its row key.
- Retrieve product metrics for a specific product, date, and hour.

## 4. Comparison of MongoDB and HBase

| Feature | MongoDB | HBase |
|---|---|---|
| Data model | Document-oriented | Wide-column |
| Data organization | Collections and BSON documents | Tables, rows, and column families |
| Main project data | Users, products, categories, transactions | Browsing events and hourly product metrics |
| Query approach | Document queries and aggregation pipelines | Row-key lookups and scans |
| Data access pattern | Flexible document retrieval and aggregation | Key-based retrieval and high-volume writes |
| Schema approach | Flexible document structure | Defined column families with flexible qualifiers |
| Best fit in this project | E-commerce entities and purchase analysis | Browsing-event storage and time-oriented product metrics |

## 5. Why Both Technologies Are Used

Using both databases allows the project to match each dataset with an appropriate storage model.

MongoDB handles the core e-commerce records and analytical aggregations. HBase handles high-volume browsing events and pre-aggregated hourly product metrics.

This separation avoids forcing all workloads into one database. The project implements batch-based cross-system integration by exporting MongoDB and HBase data and combining them using Apache Spark.

## 6. Limitations and Considerations

- MongoDB aggregation performance depends on document structure, indexes, and pipeline design.
- HBase queries are most efficient when the row key is known or can be used to define a suitable scan.
- HBase does not provide the same flexible ad hoc querying experience as MongoDB.
- Maintaining related information across two databases introduces additional data-management and integration work.
- The HBase product metrics table was historically verified with 95,140 records. The loader uses a deterministic sample selected from the full 4,174,030-row aggregation output, taking up to 20 evenly spaced records per product. This is not a random sample and should not be assumed statistically representative. The full aggregation output is retained in `hbase_project/results/product_metrics_hourly.csv` and excluded from Git.

## 7. Conclusion

MongoDB and HBase serve complementary roles in this e-commerce analytics project. MongoDB provides flexible storage and aggregation capabilities for core business entities, while HBase provides scalable storage and key-based retrieval for browsing events and hourly product metrics.

The combination demonstrates how different NoSQL databases can be selected according to data characteristics and access patterns.
