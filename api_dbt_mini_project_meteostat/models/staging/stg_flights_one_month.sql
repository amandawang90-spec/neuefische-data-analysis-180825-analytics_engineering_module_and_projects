{{ config(materialized = 'view') }}

SELECT 
    *
FROM {{ source('flights_data', 'flights') }} AS f
WHERE f.flight_date::DATE BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY f.flight_date
