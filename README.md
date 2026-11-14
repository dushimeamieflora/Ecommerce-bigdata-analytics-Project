# E-Commerce Big Data Analytics Project

## 1. Project Overview

This project focuses on applying Big Data technologies to e-commerce data. It explores how large-scale data can be generated, stored, processed, analyzed, and visualized using distributed computing and multi-model database technologies.

The project uses synthetic e-commerce data representing users, products, categories, browsing sessions, and transactions.

The main objective is to demonstrate how different Big Data technologies can work together to support scalable data storage, processing, and analytics.

The implementation uses MongoDB for document-oriented storage, Apache HBase for wide-column storage, and Apache Spark for distributed data processing and analytics.

## 2. Technologies and Tools

| Technology | Purpose |
|---|---|
| Python | Dataset generation and project development |
| MongoDB | Document-oriented data storage and aggregation |
| Apache HBase | Wide-column storage for browsing events and product metrics |
| Apache Spark | Distributed data processing and analytics |
| Pandas | Data manipulation and analysis |
| NumPy | Numerical operations |
| Matplotlib | Data visualization |
| Seaborn | Statistical visualization |
| Jupyter Notebook | Interactive data exploration |
| HappyBase | Python client for HBase Thrift |
| PyMongo | Python client for MongoDB |
| Docker Desktop | Containerized HBase environment |
| Git and GitHub | Version control and project sharing |

## 3. Project Structure

The project is organized into the following directories and files:

```text
ecommerce_bigdata_final/
│
├── .venv/                          # Python virtual environment
│
├── data/
│   ├── raw/                        # Original generated datasets
│   └── processed/                  # Cleaned and transformed datasets
│
├── docs/                           # Project documentation and diagrams
│
├── hbase_project/
│   ├── queries/                    # HBase query files
│   ├── results/                    # HBase aggregation and sample outputs
│   ├── schema/                     # HBase table definitions
│   └── scripts/                    # HBase processing and loading scripts
│
├── integration/                    # Integration between technologies
│
├── mongodb/                        # MongoDB implementation and queries
│
├── results/                        # Analytical outputs and results
│
├── spark/                          # Apache Spark processing and analytics
│
├── visualizations/                 # Charts and visual outputs
│
├── dataset_generator.ipynb         # Original dataset generator notebook
├── dataset_generator_extracted.py  # Python dataset generation script
│
├── requirements.txt                # Python dependencies
├── README.md                       # Project documentation
└── .gitignore                      # Files excluded from Git
```

## 4. Dataset Description

The project generates synthetic e-commerce data containing the following entities:

| Dataset | Description |
|---|---|
| Users | Customer information |
| Products | Product details, prices, stock, and categories |
| Categories | Product classification |
| Sessions | Customer browsing activities |
| Transactions | Customer purchases and payment information |

### 4.1 Dataset Size

The generated dataset contains the following records:

| Dataset | Number of Records |
|---|---:|
| Users | 10,000 |
| Products | 5,000 |
| Categories | 25 |
| Sessions | 2,000,000 |
| Transactions | 500,000 |

The datasets are generated in JSON and JSON Lines (JSONL) formats and stored in `data/raw/`.

## 5. Development Environment Setup

### 5.1 Prerequisites

- Windows operating system
- Python 3.13.3
- Visual Studio Code
- Docker Desktop
- Git

### 5.2 Create a Virtual Environment

Create the project virtual environment:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 5.3 Install Dependencies

Upgrade pip:

```powershell
python -m pip install --upgrade pip
```

Install the required Python packages:

```powershell
python -m pip install -r requirements.txt
```

## 6. Dataset Generation

The dataset generator is located in the project root directory.

### 6.1 Generate a Test Dataset

To generate a smaller dataset for testing:

```powershell
python dataset_generator_extracted.py --test
```

### 6.2 Generate the Full Dataset

To generate the complete dataset:

```powershell
python dataset_generator_extracted.py
```

Generated datasets are saved in:

```text
data/raw/
```

**Note:** The full dataset has already been generated and used in the MongoDB and HBase implementation. Do not rerun the generator on an existing project without first checking its output behavior, because it may overwrite or duplicate existing data.

## 7. Project Implementation

The project is organized into four main parts.

### Part 1: Data Modeling and Storage

**MongoDB**

MongoDB stores the product catalog, categories, users, and transaction records.

Implemented work includes:

- Loading 5,000 products.
- Loading 25 categories.
- Loading 10,000 users.
- Loading 500,000 transactions.
- Creating indexes to support common query patterns.
- Implementing and executing aggregation pipelines for revenue by category and top-selling products.

**Apache HBase**

HBase stores browsing events and hourly product performance metrics in two tables:

- `ecom:browsing_events`
- `ecom:product_metrics`

Implemented work includes:

- Designing the two HBase tables around analytical access patterns.
- Loading and testing browsing-event data.
- Retrieving browsing activity for a specific user.
- Aggregating product performance metrics from session and transaction data.
- Loading and verifying a representative sample of 95,140 product-metrics records.

The full product-metrics aggregation contains 4,174,030 records covering 4,757 products across 91 dates.

### Part 2: Distributed Processing with Apache Spark

The Apache Spark implementation is completed and includes:

- Cleaning and normalization of e-commerce datasets using PySpark.
- Data-quality auditing and validation of core datasets.
- Product co-purchase analysis using transaction-item combinations.
- Customer cohort analysis based on registration and transaction months.
- Spark SQL analytics covering category revenue, monthly revenue, top customers, and top products.

**Implementation scripts:**

- `spark/01_clean_normalize.py` through `spark/05_validate_core_datasets.py` — data preparation and validation.
- `spark/06_product_recommendations.py` — product co-purchase analysis.
- `spark/07_customer_cohort_analysis.py` — customer cohort analysis.
- `spark/08_verify_cohort_retention.py` — cohort activity verification.
- `spark/09_spark_sql_analytics.py` — Spark SQL analytics.

**Validated analytical results:**

- 500,000 transactions analyzed.
- 1,752,053 product units sold.
- Gross item revenue: 877,342,592.99.
- Net transaction revenue: 833,567,793.24.
- 12,628 product co-purchase pairs meeting the minimum frequency threshold.
- 34 customer registration cohorts.

**Analytical limitations:** The synthetic dataset produces 100% observed monthly cohort retention for August–October 2026. Product co-purchase frequencies are also low, so these results should not be interpreted as strong evidence of real-world customer loyalty or personalized recommendation quality.

Detailed methodology and results are documented in `docs/part2_spark_processing.md`.

### Part 3: Analytics Integration

The project implements a batch-based analytical integration workflow combining data exported from MongoDB and HBase using Apache Spark.

**Data sources:**

- MongoDB: users, products, categories, and transactions.
- HBase: browsing events and product activity metrics.
- Apache Spark: distributed transformations, aggregations, joins, and analytical outputs.

**Implementation scripts:**

- `integration/end_to_end/02_export_mongodb_collections.py` — exports MongoDB collections to JSONL.
- `integration/hbase_spark/01_export_hbase_tables.py` — exports HBase tables to JSONL.
- `integration/end_to_end/03_mongodb_hbase_spark_analysis.py` — integrates exported datasets using Spark.
- `integration/end_to_end/04_export_integrated_results_to_csv.py` — exports integrated results to CSV.
- `integration/end_to_end/05_validate_integrated_results.py` — validates integrated outputs.
- `integration/end_to_end/06_create_integrated_visualizations.py` — generates analytical visualizations.

**Validated integration results:**

- 5,000 products represented in integrated product performance.
- 10,000 customers represented in integrated customer journeys.
- 25 categories represented in integrated category performance.
- 10,301,209 browsing events and 2,000,000 distinct browsing sessions.
- 1,752,053 units sold across the transaction dataset.
- Gross item revenue: 877,342,592.99.
- Net transaction revenue: 833,567,793.24.
- Discounts explaining the difference: 43,774,799.75.

**Interpretation and limitations:**

- This is an export-based batch integration workflow, not a live federated query.
- Gross item revenue is calculated from transaction-item subtotals; net transaction revenue reflects discounts.
- 243 products have missing joined activity metrics in the integrated product output. These null values should not automatically be interpreted as zero activity.
- HBase product-metrics revenue represents a different analytical scope and should not be treated as directly equivalent to MongoDB transaction revenue.

The implementation and analytical methodology are documented in `docs/part3_analytics_integration.md`.

### Part 4: Data Visualization

The project includes six generated visualizations based on the integrated analytical results.

**Generated charts:**

- `01_top_products_by_revenue.png` - product revenue comparison.
- `02_revenue_by_category.png` - revenue distribution across categories.
- `03_customer_spending_distribution.png` - customer spending distribution.
- `04_top_customers_by_browsing.png` - browsing activity among leading customers.
- `05_hbase_product_metrics_funnel.png` - aggregate HBase product activity metrics.
- `06_product_views_vs_revenue.png` - comparison of product views and revenue.

All six charts are stored in `visualizations/integrated_analysis/`.

The HBase activity chart summarizes aggregate activity counts; it does not establish a verified customer conversion funnel.

The visualization methodology and interpretation are documented in `docs/part4_visualization_and_insights.md`.

The final technical report is available at `docs/Technical_Report.pdf`. Verification of the assignment's requirement to embed three to four meaningful charts in the PDF remains part of the final report audit.

## 8. Project Progress

### Completed

- [x] Project directory and main folders created.
- [x] Python virtual environment created.
- [x] Requirements file prepared.
- [x] Dataset generator prepared.
- [x] Test dataset generated and validated.
- [x] Full synthetic dataset generated.
- [x] MongoDB collections populated.
- [x] MongoDB indexes created.
- [x] MongoDB aggregation pipelines implemented and tested.
- [x] HBase environment configured using Docker.
- [x] HBase browsing-events table implemented and historically verified.
- [x] HBase product-metrics table implemented and historically verified.
- [x] User browsing-activity retrieval tested.
- [x] Product-metrics sample loaded and row count verified.
- [x] MongoDB and HBase implementation documentation prepared.
- [x] Spark data cleaning, normalization, and validation implemented.
- [x] Product co-purchase analysis completed.
- [x] Customer cohort analysis completed.
- [x] Spark SQL analytical outputs generated.
- [x] MongoDB-HBase-Spark batch integration implemented.
- [x] Integrated product, customer, and category results validated.
- [x] Six integrated analytical visualizations generated.
- [x] Technical report PDF prepared at `docs/Technical_Report.pdf`.

### Pending Final Verification

- [ ] Verify current HBase runtime health without deleting or recreating the existing data volume.
- [ ] Review HBase schema documentation and any remaining cleanup requirements.
- [ ] Verify performance-evaluation evidence and MongoDB-versus-HBase comparison.
- [ ] Confirm that the technical report meets the assignment's 6-8 page requirement and contains 3-4 meaningful embedded visualizations.
- [ ] Complete the README reproduction instructions and GitHub exclusions documentation.
- [ ] Perform final repository, requirements-checklist, and submission audit.

Historical HBase verification results do not establish that the HBase service is currently running. The existing Docker data volume must be preserved during troubleshooting.

## 9. Reproducibility

All commands should be executed from the project root directory.

### 9.1 Environment Setup

Create and activate a Python virtual environment, then install the project dependencies.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Apache Spark requires a compatible Java installation. MongoDB and HBase must be configured separately.

### 9.2 Dataset Generation

The dataset generator supports two modes:

**Test dataset:**

```powershell
python dataset_generator_extracted.py --test
```

**Full dataset:**

```powershell
python dataset_generator_extracted.py
```

The full dataset contains 10,000 users, 5,000 products, 25 categories, 2,000,000 sessions, and 500,000 transactions.

**Warning:** Both commands delete existing `.json` and `.jsonl` files directly inside `data/raw/` before generating new data. Do not run them against data you need to preserve.

### 9.3 MongoDB and HBase

MongoDB must be available at `localhost:27017`. HBase scripts use the Thrift service at `localhost:9090`.

MongoDB implementation scripts are located in `mongodb/`.

HBase schema definitions, ingestion scripts, and queries are located in `hbase_project/`.

Inspect existing database contents and script safeguards before running ingestion. Do not delete or recreate the existing HBase Docker data volume.

### 9.4 Apache Spark Processing

The Spark processing scripts are located in `spark/`.

The implementation includes data cleaning, normalization, validation, product co-purchase analysis, customer cohort analysis, and Spark SQL queries.

The corresponding analytical outputs are stored in `results/recommendations/`, `results/cohort_analysis/`, and `results/spark_sql/`.

### 9.5 Cross-System Integration

After MongoDB and HBase are populated and accessible, export their data:

```powershell
python integration/end_to_end/02_export_mongodb_collections.py
python integration/hbase_spark/01_export_hbase_tables.py
```

The HBase exporter supports resuming existing exports. Inspect existing export files before rerunning it.

After both sets of JSONL exports are available, execute:

```powershell
python integration/end_to_end/03_mongodb_hbase_spark_analysis.py
python integration/end_to_end/04_export_integrated_results_to_csv.py
python integration/end_to_end/05_validate_integrated_results.py
python integration/end_to_end/06_create_integrated_visualizations.py
```

The integration is batch-based. Spark reads exported MongoDB and HBase data rather than querying both databases live.

**Warning:** The integrated analysis overwrites generated output directories when rerun.

### 9.6 Outputs

- `results/integration/end_to_end/` — integrated Parquet and CSV results.
- `results/integration/validation/` — integration validation reports.
- `visualizations/integrated_analysis/` — six generated visualizations.
- `docs/Technical_Report.pdf` — technical report.

### 9.7 GitHub Data Exclusions

The `.gitignore` excludes large or regenerable local files, including:

- `data/raw/`, `data/processed/`, and `data/temp/`.
- `results/integration/mongodb_export/`.
- `results/integration/hbase_export/`.
- `hbase_project/results/product_metrics_hourly.csv`.
- Local HBase installation files, virtual environments, and temporary files.

A fresh GitHub clone therefore does not include the full raw dataset, processed datasets, database contents, or integration JSONL exports. These must be generated or recreated.

### 9.8 Verification Status

The project contains generated analytical results and validation evidence. However, complete reproduction from a fresh GitHub clone has not yet been verified.

Current HBase runtime recovery, final PDF compliance, and final submission checks remain pending.
## 10. Author

**Project:** E-Commerce Big Data Analytics

**Institution:** Adventist University of Central Africa (AUCA)

**Program:** Master of Science in Big Data Analytics