WITH daily AS (
    SELECT * FROM {{ ref('stg_weather_daily') }}
)
SELECT
    airport_code,
    DATE_TRUNC('month', date) AS month,
    AVG(avg_temp_c) AS avg_temp_c_monthly,
    SUM(precipitation_mm) AS total_precipitation_mm
FROM daily
GROUP BY 1, 2