WITH source_data AS (
       SELECT *
       FROM {{ source('northwind_data', 'northwind_order_details') }}
)
SELECT 
    order_id,
    product_id,
    unit_price::NUMERIC AS unit_price,
    quantity,
    discount::NUMERIC AS discount
FROM source_data