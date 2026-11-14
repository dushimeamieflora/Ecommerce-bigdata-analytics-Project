# Part 3: Analytics Integration

## 1. Introduction

This part integrates data stored in MongoDB and HBase using Apache Spark. The purpose is to demonstrate how information from different data storage technologies can be combined to produce broader business insights that would be difficult to obtain by examining each system independently.

MongoDB stores the e-commerce product catalogue and transaction information, while HBase stores product activity metrics. Apache Spark provides the processing environment for combining these datasets and generating integrated analytical results.

## 2. Business Question

**Which products generate sales revenue, and how does their recorded browsing engagement compare with their sales performance?**

This analysis helps an e-commerce business examine product performance using both transaction information and browsing activity.

Sales information alone shows which products generate revenue. Browsing metrics provide additional information about customer engagement with products. Combining these sources provides a broader view of product performance.

The analysis describes relationships between recorded activity and sales; it does not establish that browsing activity causes purchases.

## 3. Data Sources

The integration uses the following datasets:

| System | Dataset | Description |
|---|---|---|
| MongoDB | Products | Product catalogue and product details |
| MongoDB | Transactions | Customer purchases, quantities and transaction amounts |
| HBase | Product metrics | Product views, cart additions, purchases, quantity sold and revenue |
| Apache Spark | Integrated DataFrames | Combined and processed datasets |

The MongoDB exports contain 5,000 products and 500,000 transactions. The HBase product metrics export contains 95,140 records.

The HBase product metrics represent a deterministic sample of hourly product metrics, selected using up to 20 evenly spaced records per product. They do not represent the complete 4,174,030-record hourly metrics dataset and should not be interpreted as a statistically representative sample.

## 4. Integration and Processing Workflow

The integration was implemented using PySpark.

### Step 1: Extract data

The MongoDB collections were exported into JSONL files. HBase tables were also exported into JSONL files using the HBase integration export script.

### Step 2: Load data into Spark

Spark reads the exported datasets and creates DataFrames for the product catalogue, transactions and HBase product metrics.

### Step 3: Transform and aggregate data

Spark processes the datasets to prepare them for integration. Transaction information is summarized at product level, while HBase product activity metrics are prepared for product-level analysis.

### Step 4: Join datasets

The product catalogue, transaction summaries and HBase product metrics are combined using product identifiers.

This makes it possible to examine product details, sales performance and recorded browsing engagement in one integrated dataset.

### Step 5: Save integrated results

The integrated product performance dataset is saved in Parquet format:

`results/integration/end_to_end/integrated_product_performance`

Additional integrated datasets were also produced for customer journeys and category performance.

## 5. Integration Results and Validation

The integration produced three analytical datasets:

| Integrated dataset | Records |
|---|---:|
| Product performance | 5,000 |
| Customer journey | 10,000 |
| Category performance | 25 |

The saved integration validation reports confirmed:

| Validation measure | Result |
|---|---:|
| Products matched with MongoDB sales | 4,757 |
| Products matched with sampled HBase metrics | 4,757 |
| Products without matching sampled HBase metrics | 243 |
| Customers matched with transactions | 10,000 |
| Customers matched with HBase browsing | 10,000 |
| Categories with transaction revenue | 25 |
| Duplicate product-key groups | 0 |
| Duplicate customer-key groups | 0 |
| Duplicate category-key groups | 0 |
| Missing product IDs | 0 |
| Missing customer IDs | 0 |
| Negative product revenue rows | 0 |
| Negative product quantity rows | 0 |
| Negative customer spending rows | 0 |
| Negative category revenue rows | 0 |

The integration uses left joins from the product catalogue to the aggregated transaction and HBase metrics datasets. This retains all 5,000 products, including products without matching activity records.

The 243 products without matching sampled HBase metrics should not be interpreted as products with zero browsing activity. The absence of a match reflects the scope of the exported metrics sample.

The validated integrated measures included:

| Measure | Value |
|---|---:|
| Gross product transaction revenue | 877,342,592.99 |
| Net customer spending | 833,567,793.24 |
| Product quantity sold | 1,752,053 |
| HBase product views in exported metrics sample | 113,860 |
| HBase cart additions in exported metrics sample | 63,366 |
| Customer browsing events | 10,301,209 |
| Customer sessions | 2,000,000 |

Gross product transaction revenue is based on transaction item subtotals, whereas net customer spending uses transaction totals after discounts. HBase product activity metrics cover a sampled subset of hourly metrics and must not be treated as directly comparable to full-dataset transaction totals.

The validation reports are stored in:

- `results/integration/validation/integration_validation_report.txt`
- `results/integration/validation/integration_validation_report.csv`

## 6. Technology Selection

### MongoDB

MongoDB stores flexible e-commerce records, including product information and transaction documents. Its document-oriented structure is suitable for storing product and transaction data.

### HBase

HBase stores large volumes of product activity records. Its distributed, column-family-oriented design supports access to large datasets organized around product and time-based records.

### Apache Spark

Spark provides distributed data processing and supports reading, transforming, aggregating and joining datasets from different storage systems. It is suitable for this integration because it can process the exported MongoDB and HBase data together using DataFrames and Spark SQL.

## 7. Performance Considerations

The implemented integration uses several design choices intended to make batch processing manageable:

- **Common export format:** MongoDB and HBase records are exported to JSONL for processing by Spark.
- **Aggregation before joining:** Transactions and product metrics are summarized before product-level joins, reducing the number of records participating in those joins.
- **Left joins:** The product catalogue remains the primary dataset, retaining products without matching activity records.
- **Columnar output:** Integrated results are saved as Parquet for subsequent analysis.
- **Validation:** Duplicate-key, missing-identifier and negative-value checks help assess output consistency.

The project was executed in a local development environment. No reproducible execution-time benchmark, throughput comparison or measured speedup was identified in the reviewed results and documentation.

Accordingly, these are implementation and performance-design considerations, not measured claims that Spark, MongoDB or HBase outperformed another technology.

The HBase hourly product metrics sample also reduces the data volume used in the integrated product-metrics analysis. This improves local manageability but limits the coverage of the resulting activity measures.

## 8. Conclusion

The integration demonstrates how MongoDB, HBase and Apache Spark can work together in an e-commerce analytics environment.

MongoDB provides product and transaction information, HBase provides product activity metrics, and Spark combines these datasets to produce integrated analytical outputs.

The resulting product performance dataset contains 5,000 records, with 4,757 products matched to MongoDB sales and 4,757 products matched to the sampled HBase metrics. These matching counts do not by themselves establish that the two sets of 4,757 products are identical. The validation results show no duplicate product-key groups and no missing product identifiers.

This integrated approach provides a broader basis for understanding product performance than analyzing transaction or browsing data separately.
