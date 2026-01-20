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
      FROM {{ ref('prep_flights') }}
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
	  FROM {{ ref('prep_flights') }}
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
LEFT JOIN {{ ref('prep_airports') }} AS pa
USING (faa)
ORDER BY total_diverted_flights DESC