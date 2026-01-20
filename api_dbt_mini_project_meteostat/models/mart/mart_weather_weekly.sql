WITH daily_data AS (   
       SELECT *
       FROM {{ ref('prep_weather_daily') }}
),
weekly_aggregation AS (
       SELECT 
          da.airport_code,
          da.station_id,
          da.year AS date_year,
          da.week AS cw,
          AVG(avg_temp_c)::NUMERIC(4,2) AS avg_temp_c,
          MIN(min_temp_c)::NUMERIC(4,2) AS min_temp_c,
          MAX(max_temp_c)::NUMERIC(4,2) AS max_temp_c,
          SUM(precipitation_mm) AS total_prec_mm,
          SUM(max_snow_mm) AS total_max_snow_mm,
          AVG(avg_wind_direction)::NUMERIC(5,2) AS avg_wind_direction,
          AVG(avg_wind_speed_kmh)::NUMERIC(5,2) AS avg_wind_speed_kmh,
          MAX(wind_peakgust_kmh)::NUMERIC(5,2) AS wind_peakgust_kmh,
          AVG(avg_pressure_hpa)::NUMERIC(6,2) AS avg_pressure_hpa,
          SUM(sun_minutes) AS total_sun_minutes
       FROM daily_data AS da
       GROUP BY da.week, da.airport_code, da.station_id, da.year  
)
SELECT * 
FROM weekly_aggregation 
ORDER BY airport_code, cw
