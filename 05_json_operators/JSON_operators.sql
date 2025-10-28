SELECT extracted_data
FROM weather_daily_raw;

SELECT extracted_data -> 'data'
FROM weather_daily_raw;

-- this will not work. Because each cell in 'data' has a whole sequence, an array, of daily values. 
SELECT extracted_data -> 'data' -> 'tavg'
FROM weather_daily_raw;

-- To handle this, we use the `JSON_ARRAY_ELEMENTS` function, which expands the array into a set of rows (per day), 
-- allowing us to break down the daily data for further analysis.

-- getting one metric as a column
SELECT JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'tavg'
FROM weather_daily_raw;

-- selecting all metrics. 
-- NOTE: `->` is returning JSON data type... we would need to take care of that.
SELECT airport_code
		,station_id
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'date' AS date
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'tavg' AS avg_temp_c
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'tmin' AS min_temp_c
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'tmax' AS max_temp_c
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'prcp' AS precipitation_mm
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'snow' AS max_snow_mm
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'wdir' AS avg_wind_direction
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'wspd' AS avg_wind_speed
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'wpgt' AS avg_peakgust
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'pres' AS avg_pressure_hpa
		,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') -> 'tsun' AS sun_minutes
FROM weather_daily_raw;


-- let's break it in 2 steps. Also we now cast each metric to a fitting data type. `->>` is returning TEXT and makes casting easy.
WITH daily_raw AS (
					SELECT airport_code
							,station_id
							,JSON_ARRAY_ELEMENTS(extracted_data -> 'data') AS json_data
					FROM weather_daily_raw		
),
daily_flattened AS (
					SELECT airport_code
							,station_id
							,(json_data ->> 'date')::DATE AS date
							,(json_data ->> 'tavg')::NUMERIC AS avg_temp_c
							,(json_data ->> 'tmin')::NUMERIC AS min_temp_c
							,(json_data ->> 'tmax')::NUMERIC AS max_temp_c
							,(json_data ->> 'prcp')::NUMERIC AS precipitation_mm
							,(json_data ->> 'snow')::NUMERIC::INTEGER AS max_snow_mm
							,(json_data ->> 'wdir')::NUMERIC::INTEGER AS avg_wind_direction
							,(json_data ->> 'wspd')::NUMERIC AS avg_wind_speed
							,(json_data ->> 'wpgt')::NUMERIC AS avg_peakgust
							,(json_data ->> 'pres')::NUMERIC AS avg_pressure_hpa
							,(json_data ->> 'tsun')::NUMERIC::INTEGER AS sun_minutes
						FROM daily_raw
)
SELECT * FROM daily_flattened;

SELECT extracted_data
FROM weather_hourly_raw;

   