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
USING (category_id)