# Part 4: Visualization and Insights

## 4.1 Introduction

This part presents visual analysis of the integrated e-commerce datasets using Python, Pandas, Matplotlib, and Seaborn. The purpose of the visualizations is to communicate important patterns in product sales, category performance, customer spending, customer browsing activity, and product engagement.

The six visualizations were generated from the integrated datasets produced in Part 3. The PNG files are stored in `visualizations/integrated_analysis/`. A separate technical report is provided at `docs/Technical_Report.pdf`. These datasets contain 5,000 products, 10,000 customers, and 25 product categories. Spark was used to load and aggregate the integrated data, while Pandas was used to prepare the aggregated results for plotting. Seaborn and Matplotlib were used to create and save the charts as PNG images.

## 4.2 Visualizations and Analysis

### Figure 1: Top Products by Revenue

**File:** `01_top_products_by_revenue.png`

This bar chart presents the highest-revenue products based on transaction records. It helps identify products that contribute significantly to sales revenue.

The analysis supports product-level performance monitoring and can help management prioritize high-performing products when making inventory and marketing decisions. The ranking is calculated from the complete integrated product dataset rather than from a randomly selected sample.

### Figure 2: Revenue by Product Category

**File:** `02_revenue_by_category.png`

This chart compares transaction revenue across the 25 product categories. It provides an overview of how sales revenue is distributed across the product catalogue.

The integrated transaction data records **877,342,592.99 in gross product transaction revenue** across the categories, calculated from transaction item subtotals before discounts. The corresponding net transaction spending total is **833,567,793.24**. Comparing category-level revenue helps identify stronger and weaker revenue contributors and can support decisions about category management, promotions, and inventory planning.

### Figure 3: Customer Spending Distribution

**File:** `03_customer_spending_distribution.png`

This histogram illustrates the distribution of customer spending using the integrated customer dataset of 10,000 customers.

It helps examine how customer spending varies across the customer base and whether spending is concentrated within particular ranges. This information can support customer segmentation and targeted marketing. The chart describes the observed distribution; it does not, by itself, establish the reasons for differences in spending.

### Figure 4: Top Customers by Browsing Activity

**File:** `04_top_customers_by_browsing.png`

This bar chart displays customers with the highest recorded browsing activity. It uses the browsing-event information integrated from the HBase data.

Browsing activity provides an engagement indicator that can be considered alongside transaction information. Customers with substantial browsing activity may be useful for further analysis of the relationship between product interest and purchasing behaviour. However, browsing activity alone does not prove purchase intention.

### Figure 5: HBase Product Engagement Metrics

**File:** `05_hbase_product_metrics_funnel.png`

This chart compares recorded product engagement metrics, including product views, cart additions, and HBase purchase-related records.

The exported HBase product metrics contained 95,140 rows selected as a deterministic sample, with up to 20 evenly spaced hourly records per product. The integration validation recorded 113,860 product views and 63,366 cart additions in the exported metrics.

These measures provide an overview of recorded engagement activity. Despite the visualization filename containing the word "funnel", these totals should not be interpreted as a verified conversion funnel, conversion rates, or unique customers progressing through sequential stages. The metrics are aggregated activity totals, and the exported HBase data represents the available integration export rather than the entire original hourly metrics dataset.

### Figure 6: Product Views versus Transaction Revenue

**File:** `06_product_views_vs_revenue.png`

This scatter plot compares product views with transaction revenue for products in the integrated dataset.

The visualization helps examine whether products with more recorded views also tend to generate higher transaction revenue. It can highlight products that may deserve further investigation, such as products with substantial browsing activity but comparatively lower revenue.

The chart shows an association between two variables. It does not establish that increased views cause increased revenue. Other factors, including price, product availability, promotions, and customer preferences, may influence sales performance.

## 4.3 Key Findings

The integrated datasets and visualizations provide several useful findings:

1. **Product performance can be compared across the catalogue.** The integrated product dataset contains 5,000 products, allowing products to be ranked by transaction revenue and compared using sales and engagement measures.

2. **Category-level sales analysis is possible.** All 25 product categories had transaction revenue in the validation results. The total recorded product transaction revenue was 877,342,592.99, providing a basis for comparing category contributions.

3. **Customer behaviour can be examined from multiple perspectives.** The integrated customer dataset contains 10,000 customers and includes spending, transaction, browsing-event, and session measures. This supports analysis beyond transaction totals alone.

4. **Browsing activity provides additional product-engagement information.** The HBase export contributed product views and cart-addition metrics, allowing browsing engagement to be considered alongside MongoDB transaction records.

5. **Combining data sources provides a broader view of performance.** The product views-versus-revenue visualization demonstrates how transaction and browsing information can be examined together at product level.

## 4.4 Limitations

The visualizations are subject to several limitations:

- The HBase product metrics used in the integration are a deterministic, product-balanced temporal sample of 95,140 records selected from 4,174,030 original hourly metric records. The sample is not statistically representative of the full hourly activity dataset.
- Product views and cart additions represent recorded activity totals and should not be treated as unique users or verified conversion rates.
- The relationship between product views and revenue is observational and does not establish causation.
- Spending and engagement charts describe the available records; further statistical analysis would be needed to explain the factors influencing customer behaviour.

## 4.5 Conclusion

The visualizations demonstrate how integrated e-commerce data can be transformed into interpretable information for business analysis. By combining transaction information from MongoDB with browsing and product-engagement information from HBase through Apache Spark, the project provides a broader view of product performance, category revenue, and customer behaviour.

The use of Seaborn and Matplotlib made it possible to present the analytical results as six static visualizations. These figures complement the database implementation, Spark processing, and cross-system integration completed in the earlier parts of the project.
