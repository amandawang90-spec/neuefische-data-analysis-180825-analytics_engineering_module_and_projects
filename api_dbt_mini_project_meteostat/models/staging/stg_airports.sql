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
    r.region
FROM {{ source('flights_data', 'airports') }} AS a
JOIN {{ source('flights_data', 'regions') }} AS r
  ON a.country = r.country
