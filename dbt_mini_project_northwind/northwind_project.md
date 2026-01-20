# 🧭 Mini Project: Northwind Sales Insights with dbt

Welcome to your first independent dbt project 🎉
You’ve been learning how to clean, transform, and structure data using dbt — now it’s time to put that knowledge into practice!
----

## 📦 Business Scenario

You’ve joined Northwind Trading, a company that distributes food and beverage products worldwide.
They store their operational data in several raw tables in a warehouse (all starting with northwind_).

However, the analytics team has three big problems:

The raw data is messy — column names and data types are inconsistent.

Dashboards take too long to load because every analyst writes long SQL joins (too many manual joins).

Everyone calculates “revenue” and “profit” differently!

Your task is to build a dbt project that cleans, enriches, and aggregates the data into clear, business-ready tables.


----
##  Project Goals
| Layer       | Purpose                                 | Example Model                |
| ----------- | --------------------------------------- | ---------------------------- |
| **Staging** | Clean and rename raw data               | `staging_orders.sql`         |
| **Prep**    | Add calculated fields, joins, and logic | `prep_sales.sql`             |
| **Marts**   | Aggregate for analysis and KPIs         | `mart_sales_performance.sql` |

You’ll also add tests and optional short documentation.

----
## Dataset Overview

All tables are already available in your database, however you will only need the following:

1. orders, 
2. order_details,
3. products, and 
4. categories.


---

## Setup your project 

Please use the same dbt project and git repo already setup for dbt_meteostat.

This means:
- You do not need to change anything in the dbt_project.yml file
- But you will need to update the source.yml file in the staging folder. 
- Remember we have only created buckets for the flights and weather data, we need to do the same for northwind data.
- And the other schema.yml files in the other layers respectively.

----

## Step 1 — Staging Models
Goal: Clean and standardize raw data from the tables.

Create the following staging models in your models/staging/ folder:

```yaml
staging_customers.sql
staging_orders.sql
staging_order_details.sql
staging_products.sql
```

Each should:

- Use `{{ source() }}` to pull data from the raw tables.

- Rename columns to snake_case.

- Cast dates and numbers to the correct types.

- Keep only relevant columns.

<details> <summary>Click to reveal hints</summary>

Use Example : `{{ source('northwind', 'northwind_orders') }}` 

Always alias columns in lowercase with underscores.

Cast date fields using ::date.

Only keep columns you actually need in later layers (e.g. order_id, order_date, customer_id).

Make sure your dbt_project.yml has materialization set to table.

</details>


<details> <summary>Click to show solution staging_orders.sql</summary>

```sql
with source_data as (
    select *
    from {{ source('northwind', 'northwind_orders') }}
)

select
    order_id,
    customer_id,
    employee_id,
    order_date::date as order_date,
    required_date::date as required_date,
    shipped_date::date as shipped_date,
    ship_via as shipper_id,
    ship_city,
    ship_country
from source_data
```

</details>


<details> <summary>Click to show solution staging_order_details.sql</summary>

```sql
with source_data as (
    select *
    from {{ source('northwind', 'northwind_order_details') }}
)

select
    order_id,
    product_id,
    unit_price::numeric as unit_price,
    quantity::int as quantity,
    discount::numeric as discount
from source_data
```
</details>

<details> <summary>Click to show solution staging_products.sql</summary>

```sql
with source_data as (
    select *
    from {{ source('northwind', 'northwind_products') }}
)

select
    product_id,
    product_name,
    supplier_id,
    category_id,
    unit_price::numeric as unit_price
from source_data
```

</details>

<details> <summary>Click to show solution staging_categories.sql</summary>

```sql
with source_data as (
    select *
    from {{ source('northwind', 'northwind_categories') }}
)

select
    category_id,
    category_name
from source_data
```

</details>

----

## Step 2 — Prep Model
Goal: Join your clean staging tables and calculate key business metrics.

Create a new file:

- `models/prep/prep_sales.sql`


This model will:

Join staging_orders, staging_order_details, and staging_products

Calculate new columns:

- revenue = unit_price * quantity * (1 - discount)

- order_year, order_month

(Optional) Add category_name by joining to staging_categories

This is where “business logic” starts.


<details> <summary>Click to reveal hints</summary>

Use `{{ ref('staging_orders') }}` to reference your staging models.

You can join on `order_id` and `product_id`.

Use `extract(year from order_date)` for order_year.

Keep only relevant columns — don’t select *.

This model should now start to look like a single “sales” dataset.

</details>


<details> <summary>Click to show sample solution prep_sales.sql</summary>

```sql
with orders as (
    select * from {{ ref('staging_orders') }}
),
order_details as (
    select * from {{ ref('staging_order_details') }}
),
products as (
    select * from {{ ref('staging_products') }}
),
categories as (
    select * from {{ ref('staging_categories') }}
),
joined as (
    select
        o.order_id,
        o.customer_id,
        p.product_name,
        c.category_name,
        od.unit_price,
        od.quantity,
        od.discount,
        (od.unit_price * od.quantity * (1 - od.discount)) as revenue,
        extract(year from o.order_date) as order_year,
        extract(month from o.order_date) as order_month
    from orders o
    join order_details od using (order_id)
    join products p using (product_id)
    left join categories c using (category_id)
)
select * from joined
```

</details>



---- 

## Step 3 — Mart Model
Goal: Create a summarized table for sales performance over time.

Create:

`models/marts/mart_sales_performance.sql`


This should:

Aggregate by order_year, order_month, and category_name

Show:

- total revenue

- total number of orders

- average revenue per order

#### This is the layer that a BI tool would use.


<details> <summary>Click to reveal hints</summary>

Input = your prep_sales model → use {{ ref('prep_sales') }}

Use sum(), count(distinct order_id), avg()

Group by 1, 2, 3

You can order the output for readability

</details>


<details> <summary>Click to show solution mart_sales_performance.sql</summary>

```sql
with sales as (
    select * from {{ ref('prep_sales') }}
)

select
    order_year,
    order_month,
    category_name,
    sum(revenue) as total_revenue,
    count(distinct order_id) as total_orders,
    avg(revenue) as avg_revenue_per_order
from sales
group by 1, 2, 3
order by order_year, order_month
```

</details>

----

## Step 4 — Testing and Documentation
Goal: Make sure your final model is correct and documented.

Create a file:

`models/marts/schema.yml`


<details> <summary> Click to reveal hints</summary>

Use YAML indentation carefully (2 spaces per level).

Use tests: [not_null, unique] for important keys.

You can describe each column for clarity.

</details>

#### Example Solution —  mart schema.yml
<details> <summary>Click to show solution schema.yml</summary>

``` yaml
version: 2

models:
  - name: mart_sales_performance
    description: "Monthly sales performance by category"
    columns:
      - name: order_year
        description: "Year when the order was placed"
        tests: [not_null]
      - name: order_month
        description: "Month when the order was placed"
        tests: [not_null]
      - name: category_name
        description: "Product category"
        tests: [not_null]
      - name: total_revenue
        description: "Sum of revenue for that period and category"
        tests: [not_null]
```
</details>


----

## Step 5 — Reflection

Create a short README.md in your own project folder answering:

- What business problem does your dbt model solve?

- Which models did you build, and what does each do?

- What insights can your mart provide to Northwind?

- What was your biggest learning moment in this project?


