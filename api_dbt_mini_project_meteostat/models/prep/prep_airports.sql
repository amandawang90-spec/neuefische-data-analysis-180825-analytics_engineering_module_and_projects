{{ config(materialized = 'table') }}

WITH airports AS (
    SELECT * FROM {{ ref('stg_airports') }}
)
SELECT
   faa, 
   name, 
   city, 
   country, 
   region, 
   lat, 
   lon, 
   alt, 
   tz, 
   dst
FROM airports