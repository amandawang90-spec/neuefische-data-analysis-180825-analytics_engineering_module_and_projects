{{ config(materialized='table') }}

WITH time_reorder AS (
       SELECT 
           swh.*,
           timestamp::DATE AS date,
           DATE_PART('day', timestamp) AS day,
           DATE_PART('week', timestamp) AS week,
           DATE_PART('month', timestamp) AS month,
           DATE_PART('year', timestamp) AS year,
           TO_CHAR(timestamp, 'FMday') AS day_name,
           TO_CHAR(timestamp, 'FMweek') AS week_name,
           TO_CHAR(timestamp, 'FMmonth') AS month_name,
           timestamp::TIME AS time,
           TO_CHAR(timestamp,'HH24:MI') AS time_in_hour_minute,
           (CASE
	          WHEN timestamp::TIME >= '00:00:00' AND timestamp::TIME < '06:00:00' THEN 'night'
              WHEN timestamp::TIME >= '06:00:00' AND timestamp::TIME < '18:00:00' THEN 'day'
              WHEN timestamp::TIME >= '18:00:00' AND timestamp::TIME < '24:00:00' THEN 'evening'
            END) AS day_part
FROM {{ ref('stg_weather_hourly') }} AS swh
)
SELECT *
FROM time_reorder AS tr