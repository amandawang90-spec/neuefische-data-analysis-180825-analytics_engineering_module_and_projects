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