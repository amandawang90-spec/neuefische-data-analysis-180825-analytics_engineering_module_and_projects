WITH source_data AS (
       SELECT *
       FROM {{ source('northwind_data', 'northwind_categories') }}
)
SELECT 
    category_id,
    category_name
FROM source_data