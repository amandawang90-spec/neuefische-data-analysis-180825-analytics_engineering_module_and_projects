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
       FROM {{ ref('prep_flights') }}
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
	    LEFT JOIN {{ ref('prep_airports') }} AS pa
	           ON fstats.origin = pa.faa
	    LEFT JOIN {{ ref('prep_airports') }} AS pas
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