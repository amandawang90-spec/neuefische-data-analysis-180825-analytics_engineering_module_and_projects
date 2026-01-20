--Step 1 — Staging Models
--Goal: Clean and standardize raw data from the tables.

--Create the following staging models in your models/staging/ folder:

--staging_orders.sql
--staging_order_details.sql
--staging_products.sql
--staging_categories.sql

--Each should:

--Use {{ source() }} to pull data from the raw tables.

--Rename columns to snake_case.

--Cast dates and numbers to the correct types.

--Keep only relevant columns.

--Orders
SELECT *
FROM northwind_orders


WITH source_data AS (
       SELECT *
       FROM northwind_orders
)
SELECT 
    order_id,
    customer_id,
    employee_id,
    order_date::date as order_date,
    required_date::date as required_date,
    shipped_date::date as shipped_date,
    ship_via as shipper_id,
    ship_city,
    ship_country
FROM source_data

--Order_details
SELECT *
FROM northwind_order_details

WITH source_data AS (
       SELECT *
       FROM northwind_order_details
)
SELECT 
    order_id,
    product_id,
    unit_price::NUMERIC AS unit_price,
    quantity,
    discount::NUMERIC AS discount
FROM source_data

--Products
SELECT *
FROM northwind_products 

WITH source_data AS (
       SELECT *
       FROM northwind_products
)
SELECT 
    product_id,
    product_name,
    supplier_id,
    category_id,
    unit_price::NUMERIC AS unit_price
FROM source_data

--Categories
SELECT *
FROM northwind_categories 

WITH source_data AS (
       SELECT *
       FROM northwind_categories
)
SELECT 
    category_id,
    category_name
FROM source_data

--CTE:
WITH orders AS (
       SELECT *
       FROM northwind_orders
),
order_details AS (
       SELECT *
       FROM northwind_order_details
),
products AS (
       SELECT *
       FROM northwind_products
),
categories AS (
       SELECT *
       FROM northwind_categories
)
SELECT 
    o.order_id,
    --DATE_PART('year', o.order_date::DATE) AS order_year,
    --DATE_PART('month', o.order_date::DATE) AS order_month,
    extract(year from o.order_date::DATE ) as order_year,
    TO_CHAR(order_date::DATE, 'FMmonth') AS order_month,
    o.customer_id,
    p.product_name,
    c.category_name,
    od.unit_price,
    od.quantity,
    od.discount,
    ROUND((od.unit_price * od.quantity * (1 - od.discount))::NUMERIC,2) as revenue
FROM orders AS o
JOIN order_details AS od
USING (order_id)
JOIN products AS p
USING (product_id)
JOIN categories AS c
USING (category_id)

--Step 2 — Prep Model
--Goal: Join your clean staging tables and calculate key business metrics.

--Create a new file:

--models/prep/prep_sales.sql
--This model will:

--Join staging_orders, staging_order_details, and staging_products

--Calculate new columns:

--revenue = unit_price * quantity * (1 - discount)

--order_year, order_month

--(Optional) Add category_name by joining to staging_categories

--This is where “business logic” starts.

WITH sales AS (
      SELECT *
      FROM prep_northwind_sales
)
SELECT 
    order_year,
    order_month,
    category_name,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND((SUM(revenue)/COUNT(DISTINCT order_id))::NUMERIC, 2) AS avg_revenue_per_order
FROM sales
GROUP BY order_year, order_month, category_name

--Step 3 — Mart Model
--Goal: Create a summarized table for sales performance over time.

--Create:

--models/marts/mart_sales_performance.sql

--This should:

--Aggregate by order_year, order_month, and category_name

--Show:

--total revenue

--total number of orders

--average revenue per order

WITH orders AS (
       SELECT *
       FROM {{ ref('staging_northwind_orders') }}
),
order_details AS (
       SELECT *
       FROM {{ ref('staging_northwind_order_details') }}
),
products AS (
       SELECT *
       FROM {{ ref('staging_northwind_products') }}
),
categories AS (
       SELECT *
       FROM {{ ref('staging_northwind_categories') }}
)
SELECT 
    o.order_id,
    extract(year from o.order_date::DATE ) as order_year,
    TO_CHAR(order_date::DATE, 'FMmonth') AS order_month,
    o.customer_id,
    p.product_name,
    c.category_name,
    od.unit_price,
    od.quantity,
    od.discount,
    ROUND((od.unit_price * od.quantity * (1 - od.discount))::NUMERIC,2) as revenue
FROM orders AS o
JOIN order_details AS od
USING (order_id)
JOIN products AS p
USING (product_id)
JOIN categories AS c

--Step 4 — Testing and Documentation
--Goal: Make sure your final model is correct and documented.

--Create a file:

--models/marts/schema.yml

WITH sales AS (
      SELECT *
      FROM {{ ref('prep_northwind_sales') }}
)
SELECT 
    order_year,
    order_month,
    category_name,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND((SUM(revenue)/COUNT(DISTINCT order_id))::NUMERIC, 2) AS avg_revenue_per_order
FROM sales
GROUP BY order_year, order_month, category_name

--Step 5 — Reflection
--Create a short README.md in your own project folder answering:

--What business problem does your dbt model solve?

--Which models did you build, and what does each do?

--What insights can your mart provide to Northwind?

--What was your biggest learning moment in this project?


