--dbt_modeling_stages:

--STAGING
--stg_airports.sql:

--method 1:
SELECT 
   a.faa,
   a.name,
   a.lat,
   a.lon,
   a.alt,
   a.tz,
   a.dst,
   a.city,
   a.country,
   r2.region
FROM airports AS a
JOIN regions_2 AS r2
  ON a.country=r2.country
  
--method 2:  
WITH airports_regions_join AS (
  SELECT * 
  FROM airports
  LEFT JOIN regions_2
  USING (country)
)
SELECT * FROM airports_regions_join

  
--stg_flights_one_month.sql:

SELECT 
    *
FROM flights AS f
WHERE f.flight_date::DATE BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY f.flight_date


--stg_weather_daily.sql:

WITH daily_raw AS (
    SELECT airport_code,
           station_id,
           JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
    FROM s_jingwang.weather_daily_raw
),
daily_flattened AS (
    SELECT airport_code,
           station_id,
           (json_data ->> 'date')::DATE AS date,
           (json_data ->> 'tavg')::NUMERIC AS avg_temp_c,
           (json_data ->> 'tmin')::NUMERIC AS min_temp_c,
           (json_data ->> 'tmax')::NUMERIC AS max_temp_c,
           (json_data ->> 'prcp')::NUMERIC AS precipitation_mm,
           (json_data ->> 'snow')::NUMERIC::INTEGER AS max_snow_mm,
           (json_data ->> 'wdir')::NUMERIC::INTEGER AS avg_wind_direction,
           (json_data ->> 'wspd')::NUMERIC AS avg_wind_speed,
           (json_data ->> 'wpgt')::NUMERIC AS avg_peakgust,
           (json_data ->> 'pres')::NUMERIC AS avg_pressure_hpa,
           (json_data ->> 'tsun')::NUMERIC::INTEGER AS sun_minutes
    FROM daily_raw
)
SELECT * FROM daily_flattened

--stg_weather_hourly.sql:

WITH hourly_raw AS (
					SELECT airport_code
							,station_id
							,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
					FROM s_jingwang.weather_hourly_raw		
),
hourly_flattened AS (
					SELECT airport_code
							,station_id
							,(json_data ->> 'time')::timestamp  AS timestamp
							,(json_data ->> 'temp')::NUMERIC AS avg_temp_c
							,(json_data ->> 'dwpt')::NUMERIC AS dew_point_in_c
							,(json_data ->> 'rhum')::NUMERIC AS humidity_in_percent
							,(json_data ->> 'prcp')::NUMERIC AS precipitation_mm
							,(json_data ->> 'snow')::NUMERIC::INTEGER AS max_snow_mm
							,(json_data ->> 'wdir')::NUMERIC::INTEGER AS avg_wind_direction
							,(json_data ->> 'wspd')::NUMERIC AS avg_wind_speed
							,(json_data ->> 'wpgt')::NUMERIC AS avg_peakgust
							,(json_data ->> 'pres')::NUMERIC AS avg_pressure_hpa
							,(json_data ->> 'tsun')::NUMERIC::INTEGER AS sun_minutes
							,(json_data ->> 'coco')::NUMERIC::INTEGER AS weather_condition_code
						FROM hourly_raw
)
SELECT * FROM hourly_flattened;


--INTERMEDIATE
--prep_airports.sql:

WITH airports AS (
    SELECT * FROM stg_airports
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

--prep_flights.sql
WITH flights_adjustment AS (
       SELECT 
           sfom.*,
           TO_CHAR(flight_date, 'fm0000')::TIME AS time,
           (dep_delay * '1 minute'::interval) AS dep_delay_interval,
           (distance / 0.621371) AS distance_km
       FROM stg_flights_one_month AS sfom
)
SELECT *
FROM flights_adjustment AS fa


--prep_weather_daily.sql:
SELECT *
FROM stg_weather_daily swd 

WITH date_part AS (
       SELECT 
           swd.*,
           DATE_PART('day', date) AS day,
           DATE_PART('week', date) AS week,
           DATE_PART('month', date) AS month,
           DATE_PART('year', date) AS year
       FROM stg_weather_daily AS swd
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

--prep_weather_hourly.sql:
SELECT *
FROM stg_weather_hourly

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
FROM stg_weather_hourly AS swh
)
SELECT *
FROM time_reorder AS tr










