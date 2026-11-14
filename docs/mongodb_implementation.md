# MongoDB Implementation

## 1. Introduction

MongoDB was selected as the document-oriented database for the e-commerce analytics project. Its flexible document structure supports records containing nested objects and arrays, making it suitable for storing customer information, product information, categories, and transactions.

The project uses a MongoDB database named `ecommerce_analytics`. MongoDB Compass was used to import and inspect the users, categories, and products collections and to examine database indexes. Python and PyMongo were used to load transaction records and perform database verification.

MongoDB supports the transaction-oriented part of the project, while HBase stores browsing events and Apache Spark performs large-scale data processing and integrated analytics.

## 2. Database and Collection Design

The `ecommerce_analytics` database contains four main collections:

| Collection | Purpose |
|---|---|
| `users` | Stores customer information and activity-related attributes. |
| `categories` | Stores product categories and their embedded subcategories. |
| `products` | Stores product information, category references, prices, and stock information. |
| `transactions` | Stores transaction records across completed, delivered, and shipped statuses, including arrays of purchased items. |

### 2.1 Users Collection

The `users` collection stores customer records. Each document represents an individual user and includes a user identifier and customer-related attributes, including geographic and activity information.

The collection contains 10,000 documents.

### 2.2 Categories Collection

The `categories` collection stores category information. Each category document contains:

- `category_id`: unique identifier assigned to the category.
- `name`: category name.
- `subcategories`: an embedded array of subcategory documents.

Each embedded subcategory contains a `subcategory_id`, `name`, and `profit_margin`.

This embedded structure keeps related subcategory information within its parent category document. It also allows category information and its subcategories to be retrieved together without requiring a separate subcategory collection.

The collection contains 25 documents.

### 2.3 Products Collection

The `products` collection stores individual product records. Important fields include:

- `product_id`: product identifier.
- `name`: product name.
- `category_id`: identifier of the product's category.
- `subcategory_id`: identifier of its subcategory.
- `base_price`: current base price.
- `current_stock`: available stock quantity.
- `is_active`: indicates whether the product is active.
- `price_history`: an embedded array containing historical prices and dates.
- `creation_date`: product creation timestamp.

Products reference categories and subcategories using their identifiers rather than embedding complete category documents. This avoids repeatedly storing the same category information in every product document.

The collection contains 5,000 documents.

### 2.4 Transactions Collection

The `transactions` collection stores transaction records with completed, delivered, and shipped statuses. Each document represents a transaction and includes transaction-level information such as its identifier, user identifier, timestamp, and purchased items.

The `items` field is an array of embedded documents. Each item records information about a purchased product, including its product identifier, quantity, unit price, and subtotal.

Embedding purchased items inside the transaction document keeps the details of a purchase together. Product information can subsequently be retrieved through aggregation operations using the product identifiers.

The collection contains 500,000 documents.

## 3. Data Loading and Verification

The users, categories, and products collections were imported into MongoDB using MongoDB Compass. Transaction records were loaded programmatically using PyMongo.

The Python loading script is:

`mongodb/load_transactions.py`

The script performs the following operations:

1. Connects to the local MongoDB server and verifies the connection.
2. Selects the `ecommerce_analytics` database and `transactions` collection.
3. Checks whether the collection already contains documents to prevent an accidental duplicate import.
4. Reads transaction records from the generated JSON Lines files.
5. Inserts records in batches of 5,000 documents.
6. Creates indexes required for transaction queries.
7. Reports the number of inserted documents and the final collection count.

The database verification script is:

`mongodb/check_mongodb.py`

It connects to MongoDB, retrieves the collection names, and reports the document count in each collection.

The verified collection counts are:

| Collection | Documents |
|---|---:|
| `users` | 10,000 |
| `categories` | 25 |
| `products` | 5,000 |
| `transactions` | 500,000 |
| **Total** | **515,025** |

The script `mongodb/inspect_mongodb_transaction.py` retrieves and displays a sample transaction document. It is used to inspect the structure and data types of the transaction fields.

## 4. Indexing Strategy

Indexes were created to support frequently used lookups and queries.

The verified custom indexes are:

| Collection | Indexed field | Purpose |
|---|---|---|
| `categories` | `category_id` | Supports category lookups. |
| `products` | `product_id` | Supports product lookups and aggregation joins. |
| `transactions` | `transaction_id` | Supports transaction identification and enforces uniqueness. |
| `transactions` | `user_id` | Supports queries by customer. |
| `transactions` | `timestamp` | Supports time-based queries and analysis. |
| `transactions` | `items.product_id` | Supports queries involving purchased products. |

MongoDB also maintains its default `_id` index for every collection. The `users` collection has only this default index in the verified index information.

The `transaction_id` index is unique, preventing multiple transaction documents from having the same transaction identifier. The other custom indexes listed above are not configured as unique indexes.

These indexes support common access patterns, including retrieving transactions by user, filtering transactions by time, and looking up products referenced by transaction items.

## 5. Aggregation Pipeline: Revenue by Category

The first aggregation pipeline calculates revenue and purchased quantities for each product category.

**Business question:** Which product categories generate the highest revenue?

The pipeline is implemented in:

`mongodb/revenue_by_category.js`

### 5.1 Pipeline Operations

The aggregation performs the following operations:

1. `$unwind`: expands the `items` array so that each purchased item can be processed individually.
2. `$group`: groups the items by `product_id` and calculates total item revenue and quantity for each product.
3. `$lookup`: joins the product information using the product identifier.
4. `$unwind`: expands the matching product information.
5. `$group`: groups the product totals by `category_id` and calculates category-level revenue and quantity.
6. `$lookup`: joins the category information using the category identifier.
7. `$unwind`: expands the matching category information.
8. `$project`: selects the category identifier, category name, total revenue, and total quantity, rounding revenue to two decimal places.
9. `$sort`: sorts categories by total revenue in descending order.

The pipeline first aggregates transaction items by product and then uses the product documents to determine their categories. This approach produces category-level totals without repeatedly grouping individual transaction items directly against category information.

### 5.2 Revenue Calculation

Revenue is calculated using the `items.subtotal` values stored in transaction documents.

The calculation therefore represents the sum of purchased item subtotals before allocating any transaction-level discount across individual items. This distinction is important when interpreting the results.

The pipeline processes the transaction documents supplied to it and does not apply an initial sampling limit.

## 6. Aggregation Pipeline: Monthly Revenue by Category

The second aggregation pipeline calculates monthly revenue, purchased quantities, and transaction counts for each category.

**Business question:** How does revenue for each product category change over time?

The pipeline is implemented in:

`mongodb/monthly_revenue_by_category.js`

### 6.1 Pipeline Operations

The aggregation performs the following operations:

1. `$unwind`: expands the transaction items array.
2. `$addFields`: converts the transaction timestamp into a date and uses `$dateTrunc` to identify the corresponding month.
3. `$lookup`: joins product information using the purchased product identifier.
4. `$unwind`: expands the matching product information.
5. `$group`: groups records by category, month, and transaction identifier, calculating the revenue and quantity for each transaction within each category.
6. `$group`: groups the intermediate results by category and month, calculating total revenue, total quantity, and transaction count.
7. `$lookup`: joins category information.
8. `$unwind`: expands the matching category information.
9. `$project`: selects the category identifier, category name, month, total revenue, total quantity, and transaction count. Revenue is rounded to two decimal places and the month is formatted as `YYYY-MM`.
10. `$sort`: sorts the results by month in ascending order and revenue in descending order within each month.

### 6.2 Transaction Count Interpretation

The pipeline first groups by category, month, and transaction identifier before counting transactions at the category-month level.

Consequently, `total_transactions` represents the number of distinct transactions containing at least one product from a particular category during a particular month.

A single transaction containing products from multiple categories can contribute once to the transaction count of each relevant category. Therefore, adding transaction counts across categories does not necessarily produce the number of unique transactions in the entire database.

The pipeline processes the transaction documents supplied to it and does not apply an initial sampling limit.

## 7. Aggregation Pipeline: Top-Selling Products

The third aggregation pipeline identifies the ten products with the highest purchased quantities.

**Business question:** Which products are purchased most frequently, and what revenue and stock information is associated with them?

The pipeline is implemented in:

`mongodb/top_selling_products.js`

### 7.1 Pipeline Operations

The aggregation performs the following operations:

1. `$unwind`: expands the transaction items array.
2. `$group`: groups purchased items by product identifier and calculates total quantity and item revenue for each product.
3. `$lookup`: joins product information using the product identifier.
4. `$unwind`: expands the matching product information.
5. `$project`: selects the product identifier, product name, category identifier, total quantity, total revenue, and current stock. Revenue is rounded to two decimal places.
6. `$sort`: sorts products by total quantity in descending order.
7. `$limit`: returns the ten products with the highest purchased quantities.

The `$limit` stage is applied at the end of the pipeline, after product totals have been calculated and sorted. It limits the returned result to ten products; it does not limit the transaction documents processed by the pipeline.

The pipeline therefore analyzes the transaction documents supplied to it rather than selecting an initial sample of 10,000 transactions.

### 7.2 Revenue Interpretation

Product revenue is calculated by summing the `items.subtotal` values associated with each product. As with the category revenue pipeline, these values represent item subtotals before transaction-level discounts are allocated to individual items.

## 8. Summary

MongoDB provides the transaction-oriented storage and aggregation component of the e-commerce analytics architecture.

The implementation demonstrates:

- Document modeling through four collections.
- Embedded subcategory, price-history, and transaction-item structures.
- Reference-based relationships between products and categories.
- Programmatic transaction loading with PyMongo and batch insertion.
- Indexing for common lookup and filtering operations.
- Aggregation pipelines for category revenue, monthly category performance, and top-selling products.
- Database verification through collection counts and sample document inspection.

The MongoDB aggregation outputs contribute to the broader analytical work presented in the project's visualization and insights documentation.

MongoDB is used alongside HBase and Apache Spark. The roles of these technologies and their integrated analytical workflows are discussed in the corresponding project documentation.
