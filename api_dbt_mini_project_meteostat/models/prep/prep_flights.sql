{{ config(materialized='table') }}

WITH flights_adjustment AS (
       SELECT 
           sfom.*,
           TO_CHAR(flight_date, 'fm0000')::TIME AS time,
           (dep_delay * '1 minute'::interval) AS dep_delay_interval,
           (distance / 0.621371) AS distance_km
       FROM {{ ref('stg_flights_one_month') }} AS sfom
)
SELECT *
FROM flights_adjustment AS fa