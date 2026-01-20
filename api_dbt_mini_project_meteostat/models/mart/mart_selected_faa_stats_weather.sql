WITH departures AS ( 
       SELECT 
          flight_date,
          origin AS faa,
	      COUNT(DISTINCT dest) AS total_departure_connections,
		  COUNT(sched_dep_time) AS total_planned_departure_flights,
		  SUM(cancelled) AS total_cancelled_departure_flights,
		  SUM(diverted) AS total_diverted_departure_flights,
		  COUNT(dep_time) AS total_actual_departure_flights,
		  COUNT(DISTINCT tail_number) AS total_unique_departure_airplanes,
	      COUNT(DISTINCT airline) AS total_unique_departure_airlines
      FROM {{ ref('prep_flights') }}
      GROUP BY origin,flight_date
      ORDER BY origin, flight_date
),
arrivals AS (
      SELECT 
         flight_date,
         dest AS faa,
		 COUNT(DISTINCT origin) AS tota_arrival_connections,
	     COUNT(sched_arr_time) AS total_planned_arrival_flights,
	     SUM(cancelled) AS total_cancelled_arrival_flights,
		 SUM(diverted) AS total_diverted_arrival_flights,
		 COUNT(arr_time) AS total_actual_arrival_flights,
		 COUNT(DISTINCT tail_number) AS total_unique_arrival_airplanes,
		 COUNT(DISTINCT airline) AS total_unique_arrival_airlines
	  FROM {{ ref('prep_flights') }}
	  GROUP BY dest, flight_date
	  ORDER BY dest, flight_date
),
total_stats AS (
	SELECT 
	    flight_date,
	    faa,
	    total_departure_connections,
	    tota_arrival_connections,
	    (total_departure_connections + tota_arrival_connections)::NUMERIC/2 AS total_connections,
	    total_planned_departure_flights + total_planned_arrival_flights AS total_planned_flights,
	    total_cancelled_departure_flights + total_cancelled_arrival_flights AS total_cancelled_flights,
	    total_diverted_departure_flights + total_diverted_arrival_flights AS total_diverted_flights,
	    ((total_planned_departure_flights + total_planned_arrival_flights + total_diverted_departure_flights+ total_diverted_arrival_flights)::NUMERIC/(total_planned_arrival_flights + total_planned_arrival_flights)::NUMERIC)*100 AS percent_change, 
	    total_actual_departure_flights + total_actual_arrival_flights AS total_actual_flights,
	    (total_unique_departure_airplanes + total_unique_arrival_airplanes)::NUMERIC/2 AS avg_total_airplanes,
	    (total_unique_departure_airlines + total_unique_arrival_airlines)::NUMERIC/2 AS avg_total_airlines
	FROM departures
	JOIN arrivals
	USING (flight_date, faa)
),
add_names AS (
    SELECT 
       ts.flight_date,
       ts.faa,
       pa.name,
       pa.city,
       pa.country,
       ts.total_departure_connections,
       ts.tota_arrival_connections,
       ts.total_connections,
       ts.total_planned_flights,
       ts.total_cancelled_flights,
       ts.total_diverted_flights,
       ts.percent_change,
       ts.total_actual_flights,
       ts.avg_total_airplanes,
       ts.avg_total_airlines
   FROM total_stats AS ts
   LEFT JOIN {{ ref('prep_airports') }} AS pa
   USING (faa)
)
SELECT 
    an.*,
    pwd.min_temp_c AS daily_min_temperature,
    pwd.max_temp_c AS daily_max_temperature,
    pwd.precipitation_mm AS daily_precipitation,
    pwd.max_snow_mm AS daily_snow_fall,
    pwd.avg_wind_direction AS daily_average_wind_direction,
    pwd.avg_wind_speed_kmh AS daily_average_wind_speed,
    pwd.wind_peakgust_kmh AS daily_wind_peakgust
FROM add_names AS an
JOIN {{ ref('prep_weather_daily') }} AS pwd
  ON an.faa=pwd.airport_code
ORDER BY an.total_diverted_flights DESC