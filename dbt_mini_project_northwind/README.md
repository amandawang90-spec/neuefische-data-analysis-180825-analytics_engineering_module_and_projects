# 🧭 Northwind Sales Insights — dbt Project

## Business Problem

Northwind Trading distributes food and beverage products worldwide but faced three key analytics challenges:

- **Inconsistent raw data** — column names and data types were messy across source tables
- **Slow dashboards** — analysts were writing long, repetitive SQL joins manually
- **No shared definition of revenue** — every team calculated it differently

This dbt project solves all three by building a clean, layered transformation pipeline that standardises the data, encodes business logic in one place, and delivers aggregated, BI-ready tables.

---

## Project Structure

```
models/
├── staging/
│   ├── staging_northwind_orders.sql
│   ├── staging_northwind_order_details.sql
│   ├── staging_northwind_products.sql
│   ├── staging_northwind_categories.sql
│   └── staging_sources.yml
├── prep/
│   ├── prep_northwind_sales.sql
│   └── schema.yml
└── marts/
    ├── mart_northwind_sales_performance.sql
    └── schema.yml
```

---

## Models

### 🔵 Staging Layer — Clean & Standardise

Each staging model pulls from a raw source table using `{{ source() }}`, renames columns to `snake_case`, casts data types, and keeps only the columns needed downstream.

| Model | Source Table | Key Transformations |
|---|---|---|
| `staging_northwind_orders` | `northwind_orders` | Cast dates with `NULLIF` safety, rename `ship_via` → `shipper_id` |
| `staging_northwind_order_details` | `northwind_order_details` | Cast `unit_price` and `discount` to `NUMERIC` |
| `staging_northwind_products` | `northwind_products` | Cast `unit_price` to `NUMERIC` |
| `staging_northwind_categories` | `northwind_categories` | Select `category_id` and `category_name` |

---

### 🟡 Prep Layer — Join & Enrich

**`prep_northwind_sales`** joins all four staging models into a single sales dataset and adds calculated business fields:

- `revenue` = `unit_price × quantity × (1 - discount)`, rounded to 2 decimal places
- `order_year` extracted from `order_date`
- `order_month` formatted as a full month name (e.g. `january`)

This is the single source of truth for revenue logic across the project.

---

### 🟢 Mart Layer — Aggregate for BI

**`mart_northwind_sales_performance`** aggregates the prep model for use in dashboards and reporting:

| Column | Description |
|---|---|
| `order_year` | Year the order was placed |
| `order_month` | Month the order was placed |
| `category_name` | Product category |
| `total_revenue` | Sum of revenue for the period and category |
| `total_orders` | Count of distinct orders |
| `avg_revenue_per_order` | Total revenue ÷ total orders, rounded to 2 dp |

---

## Tests

The mart schema includes `not_null` tests on all key columns (`order_year`, `order_month`, `category_name`, `total_revenue`) to catch data quality issues before they reach the BI layer.

Run tests with:

```bash
dbt test
```

---

## Insights This Model Enables

- **Which product categories generate the most revenue?**
- **How does revenue trend month-over-month or year-over-year?**
- **Which months have the highest average order value?**
- **Are there seasonal patterns in category performance?**

---

## Setup

This project runs inside the existing `dbt_meteostat` dbt project. No changes to `dbt_project.yml` are needed — the `northwind_data` source is already registered in `staging_sources.yml` pointing to the `s_jingwang` schema.

To build all Northwind models:

```bash
dbt run --select staging_northwind* prep_northwind* mart_northwind*
```

---

## Biggest Learning Moment

Working through this project highlighted how important the **prep layer** is as a separation of concerns — it's the right place to encode business logic like revenue calculation, so that the mart layer stays clean and focused purely on aggregation. Keeping logic in one place means analysts always get the same number, regardless of which dashboard they're looking at.
