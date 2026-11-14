# Part 2: Data Processing with Apache Spark

## 1. Introduction

Part 2 uses Apache Spark and PySpark to process and analyze the generated e-commerce dataset. The objective is to demonstrate distributed data processing, data cleaning and normalization, cohort analysis, and Spark SQL analytics.

## 2. Spark Batch Processing

The project uses PySpark DataFrames for batch processing of the generated e-commerce data.

The implemented processing scripts include:

| Script | Responsibility |
|---|---|
| `spark/01_clean_normalize.py` | Clean users, products, and categories; convert data types and remove duplicate identifiers |
| `spark/02_clean_transactions.py` | Normalize transaction financial fields and remove duplicate transaction identifiers |
| `spark/03_clean_sessions.py` | Process session data and generate Parquet output |
| `spark/04_audit_missing_values.py` | Audit source data for missing values |
| `spark/05_validate_core_datasets.py` | Validate source dataset structure and required fields |

Processed datasets are stored in `data/processed/` in Parquet format.

The cleaning workflow standardizes selected data types, removes duplicate records based on identifiers, and prepares datasets for subsequent analytics.

## 3. Product Recommendation Analysis

The project implements product co-purchase analysis using PySpark.

The script `spark/06_product_recommendations.py` reads cleaned transaction and product data, identifies products appearing together in transactions, and aggregates product-pair frequencies.

The previously inspected analytical output contained 12,628 product pairs. The maximum observed co-purchase count was four, indicating sparse product-pair relationships in the generated dataset.

This is a product-to-product co-purchase recommendation approach, not a personalized recommendation model. The low co-purchase frequencies limit the strength of individual recommendations.

Output: `results/recommendations/product_pairs/` (Parquet).

## 4. Customer Cohort Analysis

The project implements registration-month cohort analysis using `spark/07_customer_cohort_analysis.py`.

Customers are grouped by registration month, and their subsequent transaction activity is analyzed using:

- Cohort month and activity month
- Month number relative to registration
- Active customers and transaction count
- Revenue and average spending
- Cohort size and retention percentage

Previously inspected results contained 34 registration cohorts and 102 cohort-month observations.

The generated dataset showed 100% observed retention for the analyzed August–October 2026 activity period. This should be interpreted as a characteristic of the synthetic dataset and its observation window, not evidence of realistic customer retention.

The companion script `spark/08_verify_cohort_retention.py` provides additional cohort verification.

Outputs:

- `results/cohort_analysis/monthly_cohort/`
- `results/cohort_analysis/retention_matrix/`

Both outputs use Parquet format.

## 5. Spark SQL Analytics

The script `spark/09_spark_sql_analytics.py` implements four analyses:

| Analysis | Purpose | Output directory |
|---|---|---|
| Revenue by category | Compare category-level gross item subtotals | `results/spark_sql/category_revenue/` |
| Monthly revenue | Analyze monthly net transaction totals | `results/spark_sql/monthly_revenue/` |
| Top customers | Rank customers by purchasing activity | `results/spark_sql/top_customers/` |
| Top-selling products | Rank products by sales performance | `results/spark_sql/top_products/` |

Previously inspected Spark SQL results reported:

| Revenue measure | Amount |
|---|---:|
| Gross transaction item subtotals | 877,342,592.99 |
| Transaction-level discounts | 43,774,799.75 |
| Net transaction totals | 833,567,793.24 |

The monthly net revenue results were:

| Month | Net revenue |
|---|---:|
| August 2026 | 273,938,603.23 |
| September 2026 | 279,202,575.75 |
| October 2026 | 280,426,614.26 |

Gross item subtotals and net transaction totals are different measures and must not be compared without accounting for discounts.

The top-products output previously contained 12 records because of tied rankings; it should not be described as exactly 10 products.

## 6. MongoDB and HBase Data with Spark

Spark was also used to work with data originating from MongoDB and HBase.

MongoDB datasets included:

- 10,000 users
- 5,000 products
- 25 categories
- 500,000 transactions

HBase datasets included:

- 10,301,209 browsing events
- 95,140 exported product metrics records

The datasets were exported and loaded into Spark for integrated processing. Spark combined information from the different sources to produce integrated product, customer, and category performance datasets.

## 7. Integrated Processing Outputs

The following integrated Parquet datasets were successfully generated:

| Dataset | Records |
|---|---:|
| Integrated product performance | 5,000 |
| Integrated customer journey | 10,000 |
| Integrated category performance | 25 |

The integrated datasets were also exported successfully to CSV format for easier inspection and further analysis.

## 8. Validation Results

The integrated datasets were validated using a dedicated PySpark validation script.

The validation confirmed:

- 5,000 integrated product records.
- 10,000 integrated customer records.
- 25 integrated category records.
- No duplicate product, customer, or category keys.
- No missing product IDs or customer IDs.
- 4,757 products matched to MongoDB sales.
- 4,757 products matched to HBase product metrics.
- All 10,000 customers matched to transaction and browsing data.
- All 25 categories had transaction revenue.
- No negative product revenue, product quantities, customer spending, or category revenue rows.

The validation reports were saved in:

`results/integration/validation/`

Files:
- `integration_validation_report.txt`
- `integration_validation_report.csv`

## 9. Processing Outputs and Reproducibility

The project maintains separate folders for Spark applications, integration scripts, analytical results, and validation outputs.

Important scripts include:

- `spark/09_spark_sql_analytics.py`
- `integration/01_mongodb_to_spark.py`
- `integration/end_to_end/02_export_mongodb_collections.py`
- `integration/hbase_spark/01_export_hbase_tables.py`
- `integration/end_to_end/03_mongodb_hbase_spark_analysis.py`
- `integration/end_to_end/04_export_integrated_results_to_csv.py`
- `integration/end_to_end/05_validate_integrated_results.py`

These scripts provide a reproducible record of the processing and integration workflow.

## 10. Conclusion

Part 2 demonstrates the use of Apache Spark for e-commerce data processing and analytical queries. The project implemented cohort analysis, Spark SQL analytics, and processing of data originating from MongoDB and HBase.

The resulting integrated datasets were successfully generated, exported, and validated. These outputs provide the foundation for the integrated analytical query in Part 3 and the visualizations and business insights in Part 4.
