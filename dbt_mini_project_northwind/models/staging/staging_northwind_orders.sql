WITH source_data AS (
       SELECT *
       FROM {{ source('northwind_data', 'northwind_orders') }}
)
SELECT 
    order_id,
    customer_id,
    employee_id,
    NULLIF(order_date, 'NULL')::date AS order_date,
    NULLIF(required_date, 'NULL')::date AS required_date,
    NULLIF(shipped_date, 'NULL')::date AS shipped_date,
    ship_via as shipper_id,
    ship_city,
    ship_country
FROM source_data