5.1 Airport Stats
In a table mart_faa_stats.sql we want to see for each airport over all time:

unique number of departures connections

unique number of arrival connections

how many flight were planned in total (departures & arrivals)

how many flights were canceled in total (departures & arrivals)

how many flights were diverted in total (departures & arrivals)

how many flights actually occured in total (departures & arrivals)

(optional) how many unique airplanes travelled on average

(optional) how many unique airlines were in service on average

add city, country and name of the airport

SELECT *
FROM flights

SELECT *
FROM airports

SELECT *
FROM prep_flights


SELECT *
FROM prep_flights
WHERE dep_delay IS NOT NULL 
ORDER BY dep_delay DESC


SELECT 
    DISTINCT dest,
    COUNT(*) AS total_unique_arrival_connections
FROM prep_flights
GROUP BY dest



WITH departures AS ( 
       SELECT 
          origin AS faa,
	      COUNT(DISTINCT dest) AS total_departure_connections,
		  COUNT(sched_dep_time) AS total_planned_departure_flights,
		  SUM(cancelled) AS total_cancelled_departure_flights,
		  SUM(diverted) AS total_diverted_departure_flights,
		  COUNT(dep_time) AS total_actual_departure_flights,
		  COUNT(DISTINCT tail_number) AS total_unique_departure_airplanes,
	      COUNT(DISTINCT airline) AS total_unique_departure_airlines
      FROM prep_flights
      GROUP BY origin
      ORDER BY origin
),
arrivals AS (
      SELECT 
         dest AS faa,
		 COUNT(DISTINCT origin) AS tota_arrival_connections,
	     COUNT(sched_arr_time) AS total_planned_arrival_flights,
	     SUM(cancelled) AS total_cancelled_arrival_flights,
		 SUM(diverted) AS total_diverted_arrival_flights,
		 COUNT(arr_time) AS total_actual_arrival_flights,
		 COUNT(DISTINCT tail_number) AS total_unique_arrival_airplanes,
		 COUNT(DISTINCT airline) AS total_unique_arrival_airlines
	  FROM prep_flights
	  GROUP BY dest
	  ORDER BY dest
),
total_stats AS (
	SELECT 
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
	USING (faa)
)
SELECT 
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
LEFT JOIN prep_airports AS pa
USING (faa)
ORDER BY total_diverted_flights DESC	
	

--mart_faa_stats

WITH departures AS ( 
	SELECT origin AS faa
			,COUNT(DISTINCT dest) AS nunique_to
			,COUNT(sched_dep_time) AS dep_planned
			,SUM(cancelled) AS dep_cancelled
			,SUM(diverted) AS dep_diverted
			,COUNT(arr_time) AS dep_n_flights
			,COUNT(DISTINCT tail_number) AS dep_nunique_tails -- BONUS TASK
		    ,COUNT(DISTINCT airline) AS dep_nunique_airlines -- BONUS TASK
	FROM prep_flights
	GROUP BY origin
	ORDER BY origin
),
arrivals AS (
	SELECT dest AS faa
			,COUNT(DISTINCT origin) AS nunique_from
			,COUNT(sched_dep_time) AS arr_planned
			,SUM(cancelled) AS arr_cancelled
			,SUM(diverted) AS arr_diverted
			,COUNT(arr_time) AS arr_n_flights
			,COUNT(DISTINCT tail_number) AS arr_nunique_tails 
			,COUNT(DISTINCT airline) AS arr_nunique_airlines
	FROM prep_flights 
	GROUP BY dest
	ORDER BY dest
),
total_stats AS (
	SELECT faa
			,nunique_to
			,nunique_from
	        ,(nunique_to + nunique_from)::NUMERIC/2 AS n_connections -- fractions would indicate that for the number of connections to!=from 
			,dep_planned + arr_planned AS total_planned
			,dep_cancelled + arr_cancelled AS total_cancelled
			,dep_diverted + arr_diverted AS total_diverted
	        ,((dep_cancelled + arr_cancelled + dep_diverted + arr_diverted)::NUMERIC/(dep_planned + arr_planned)::NUMERIC)*100 AS percent_change 
			,dep_n_flights + arr_n_flights AS total_flights
	        ,(dep_nunique_tails + arr_nunique_tails)::NUMERIC/2 AS nunique_tails 
	        ,(dep_nunique_airlines + arr_nunique_airlines)::NUMERIC/2 AS nunique_airlines 
	FROM departures
	JOIN arrivals
	USING (faa)
)
SELECT 
   a.city,
   a.country,
   a.name,
   t.* 
FROM total_stats AS t
LEFT JOIN prep_airports AS a
USING (faa)
ORDER BY total_diverted DESC


5.2 Flight Route Stats
In a table mart_route_stats.sql we want to see for each route over all time:

origin airport code
destination airport code
total flights on this route
unique airplanes
unique airlines
on average what is the actual elapsed time
on average what is the delay on arrival
what was the max delay?
what was the min delay?
total number of cancelled
total number of diverted
add city, country and name for both, origin and destination, airports

SELECT *
FROM prep_airports 

SELECT *
FROM prep_flights


SELECT 
    origin,
    dest,
    COUNT(flight_number) AS total_flights,
    COUNT(DISTINCT tail_number) AS total_unique_airplanes,
    COUNT(DISTINCT airline) AS total_unique_airlines,
    AVG(actual_elapsed_time) AS avg,
    AVG(actual_elapsed_time)::INTEGER AS avg_int,
    (AVG(actual_elapsed_time)::INTEGER * INTERVAL '1 second') AS avg_int,
    AVG(actual_elapsed_time)::INTEGER * ('1 second'::INTERVAL) AS avg_actual_elapsed_time,
    AVG(arr_delay)::INTEGER * ('1 second'::INTERVAL) AS avg_arr_delay,
    --make_interval(secs => AVG(actual_elapsed_time)::INTEGER) AS avg_actual_elapsed_time_1,
    --make_interval(secs => AVG(arr_delay)::INTEGER) AS avg_arr_delay_1,
    justify_interval(make_interval(mins => AVG(actual_elapsed_time)::INTEGER)) AS avg_actual_elapsed_time_2,
    justify_interval(make_interval(mins => AVG(arr_delay)::INTEGER)) AS avg_arr_delay_2
FROM prep_flights 
GROUP BY origin, dest


WITH flights_stats AS (
       SELECT 
          origin,
          dest,
          COUNT(flight_number) AS total_flights,
          COUNT(DISTINCT tail_number) AS total_unique_airplanes,
          COUNT(DISTINCT airline) AS total_unique_airlines,
          (AVG(actual_elapsed_time)::INTEGER * INTERVAL '1 second') AS avg_actual_elapsed_time,
          (AVG(arr_delay)::INTEGER * INTERVAL '1 second') AS avg_arr_delay,
          (MIN(arr_delay)::INTEGER * INTERVAL '1 second') AS min_arr_delay,
          (MAX(arr_delay)::INTEGER * INTERVAL '1 second') AS max_arr_delay,
          SUM(cancelled) AS total_cancelled,
	      SUM(diverted) AS total_diverted
       FROM prep_flights 
       GROUP BY origin, dest
),
add_names AS (
        SELECT 
            pa.name AS origin_name,
			pas.name AS dest_name,
            pa.city AS origin_city,
			pas.city AS dest_city,
			pa.country AS origin_country,
			pas.country AS dest_country,
			fstats.*
	    FROM flights_stats AS fstats
	    LEFT JOIN prep_airports AS pa
	           ON fstats.origin = pa.faa
	    LEFT JOIN prep_airports AS pas
	           ON fstats.dest = pas.faa    
)
SELECT 
    origin,
    origin_name,
    origin_city,
    origin_country,
    dest,
    dest_name,
    dest_city,
    dest_country,
    total_flights,
    total_cancelled,
    total_diverted,
    total_unique_airlines,
    total_unique_airplanes,
    avg_actual_elapsed_time,
    avg_arr_delay,
    min_arr_delay,
    max_arr_delay
FROM add_names
ORDER BY origin, dest DESC

5.3 Flight Route Stats incl. Weather
In a table mart_selected_faa_stats_weather.sql we want to see for each airport daily:

only the airports we collected the weather data for
unique number of departures connections
unique number of arrival connections
how many flight were planned in total (departures & arrivals)
how many flights were canceled in total (departures & arrivals)
how many flights were diverted in total (departures & arrivals)
how many flights actually occured in total (departures & arrivals)
(optional) how many unique airplanes travelled on average
(optional) how many unique airlines were in service on average
(optional) add city, country and name of the airport
daily min temperature
daily max temperature
daily precipitation
daily snow fall
daily average wind direction
daily average wind speed
daily wnd peakgust


SELECT *
FROM mart_faa_stats

SELECT *
FROM prep_weather_daily 


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
      FROM prep_flights
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
	  FROM prep_flights
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
   LEFT JOIN prep_airports AS pa
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
JOIN prep_weather_daily AS pwd
  ON an.faa=pwd.airport_code
ORDER BY an.total_diverted_flights DESC

5.4 Weekly weather
In a table mart_weather_weekly.sql we want to see all weather stats from the prep_weather_daily model aggregated weekly.

consider whether the metric should be Average, Maximum, Minimum, Sum or MODE

SELECT *
FROM prep_weather_daily 

WITH daily_data AS (   
       SELECT *
       FROM prep_weather_daily 
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