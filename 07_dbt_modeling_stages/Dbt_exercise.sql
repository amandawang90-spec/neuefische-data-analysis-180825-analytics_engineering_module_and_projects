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
  
  
SELECT 
    *
FROM flights AS f
WHERE f.flight_date::DATE BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY f.flight_date

WITH airports_regions_join AS (
  SELECT * 
  FROM airports
  LEFT JOIN regions
  USING (country)
)
SELECT * FROM airports_regions_join




