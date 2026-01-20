{{ config(materialized='table') }}

WITH date_part AS (
       SELECT 
           swd.*,
           DATE_PART('day', date) AS day,
           DATE_PART('week', date) AS week,
           DATE_PART('month', date) AS month,
           DATE_PART('year', date) AS year
       FROM {{ ref('stg_weather_daily') }} AS swd
),
day_month_part AS (
       SELECT 
          dp.*,
          TO_CHAR(dp.date, 'FMday') AS day_name,
          TO_CHAR(dp.date, 'FMweek') AS week_name,
          TO_CHAR(dp.date, 'FMmonth') AS month_name
       FROM date_part AS dp
 ),
 season_part AS (
        SELECT 
            dmp.*,
            (CASE
                WHEN month_name IN ('march','april','may') THEN 'spring'
                WHEN month_name IN ('june','july','august') THEN 'summer'
                WHEN month_name IN ('september','october','november') THEN 'autumn'
                WHEN month_name IN ('december','january','february') THEN 'winter'
            END) AS season
        FROM day_month_part AS dmp
)
SELECT *
FROM season_part AS sp
ORDER BY sp.season