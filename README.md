# ecommerce-analytics-dbt
E-commerce Analytics Pipeline built with DBT: Customer Segmentation, Product Performance &amp; Cohort Analysis
# E-commerce Analytics Pipeline

## Project Overview
This dbt project transforms raw e-commerce data into actionable business insights using Google BigQuery as the data warehouse.

## Architecture
- **Staging**: Raw data cleaning and standardization
- **Intermediate**: Business logic and transformations
- **Marts**: Final analytics tables optimized for reporting

## Key Features
- Customer segmentation using RFM analysis
- Product performance and ABC classification
- Cohort analysis and retention tracking
- Daily sales summaries and trends

## BigQuery Optimization
- Partitioned tables by date
- Clustered tables for query performance
- Incremental materializations
- Proper data types and constraints

## Running the Project
```bash
# Install dependencies
dbt deps

# Load seed data
dbt seed

# Run all models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate


Key Models

dim_customers: Customer dimension with segmentation
dim_products: Product dimension with performance metrics
fct_orders: Order fact table with enriched data
customer_segments: Customer segmentation analysis
cohort_analysis: Retention and lifetime value analysis