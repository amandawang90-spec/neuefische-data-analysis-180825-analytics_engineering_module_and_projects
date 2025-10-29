# Lesson B — Building Intermediate and Mart Models

## Objective
By the end of this lesson, you will:
- Understand the role of **intermediate** layers in dbt.
- Combine staging models into analytical models.
- Create basic reusable transformations for reporting or feature engineering.

> **Layer Purpose:**  
> The **prep layer** takes cleaned **staging tables** and applies **business logic or feature engineering**.  
> You should:
> - Derive new columns (dates, intervals, categories)  
> - Join staging tables if needed  
> - Prepare data for analysis or marts  
> Avoid: extremely complex business logic or heavy aggregations at this stage.
---

You’ll create four prep models:

1. prep_airports

2. prep_weather_daily

3. prep_weather_hourly

4. prep_flights

## 1. From Staging to Business Logic

We now move beyond cleaning data to actually preparing it for analysis.  
Staging models provide **clean columns**, while intermediate and mart layers give **business meaning** to the data.

---

## 2. Intermediate Models (prep)

Intermediate models sit between staging and marts. They apply logic such as:
- Joining tables (e.g., merging weather and station metadata)
- Creating reusable calculations (e.g., average monthly temperature)
- Filtering, grouping, or adding derived metrics

Example: `models/prep/prep_weather_summary.sql`

- Create a prep folder in the models folder
- Create a file `prep_weather_summary.sql`
- Paste the following code 

```sql
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
```
> This gives you one row per airport per month.

- Create a yml file for the prep models for documentation purposes.

Schema (`models/prep/schema.yml`):
```yaml
version: 2

models:
  - name: prep_weather_summary
    description: "Monthly aggregates of average temperature and precipitation per airport."
```

----

## 1. Prep Model: Airports

Objective: Reorder columns from staging_airports so that region comes after country. You should explicitly list all columns in your SELECT statement.
Order of columns: `faa, name, city, country, region, lat, lon, alt, tz, dst `

#### Task Instructions

1. Create `prep_airports.sql`.

2. Reference the staging model, you can check your columns from the following code snippet and build your CTE:
```sql
SELECT *
FROM {{ ref('staging_airports') }} -- note we do not use the source() any more but ref 
```
> For prep tables we are not linking any source tables rather tables from the staging layers, so it would be appropriate to use the ref() rather than the source()

3. Reorder columns manually in your SELECT to place region after country.

4. `dbt run --select prep_airports`

5. Check your column order in your database.


<details> 
  <summary>ONLY IF STUCK! Click for semi-filled CTE</summary>

    Use this skeleton and fill in the blanks ... to make it work.

```sql
WITH airports_reorder AS (
    SELECT faa
    	   ,...
    	   ...
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder
``` 
</details> 

----

# 2. Prep Model: Daily Weather

**Objective:** Transform `staging_weather_daily` into `prep_weather_daily` 

- Adding derived calendar features (day, month, year, week, etc.)

- Adding textual features (month_name, weekday)

- Creating a seasonal grouping

### Expected Output Columns

| Column | Description |
|--------|------------|
| airport_code | IATA airport code |
| station_id | Weather station identifier |
| date | Date of observation |
| avg_temp_c | Average temperature (°C) |
| min_temp_c | Minimum temperature (°C) |
| max_temp_c | Maximum temperature (°C) |
| precipitation_mm | Precipitation (mm) |
| sun_minutes | Sunshine duration |
| date_day | Day of month (numeric) |
| date_month | Month of year (numeric) |
| date_year | Year |
| cw | Calendar week |
| month_name | Name of month |
| weekday | Name of weekday |
| season | Winter, Spring, Summer, Autumn |

### Task Instructions

1. Create a new file: `prep_weather_daily.sql` in your `prep` directory.
2. Reference the staging model using Jinja:

```sql
SELECT *
FROM {{ ref('staging_weather_daily') }}
```

3. Using CTEs, derive:

    - numeric features → `DATE_PART('day', date)` etc.

    - text features → `TO_CHAR(date, 'FMmonth')`, `TO_CHAR(date, 'FMday')`.

5. Use CASE WHEN statements to assign seasons:

```sql
CASE
    WHEN month_name IN ('december','january','february') THEN 'winter'
    WHEN month_name IN ('march','april','may') THEN 'spring'
    ...
    ...
END AS season
```


>Build Step-by-Step:
> 1. Create only the DATE_PART() features and run.
> 2. Add TO_CHAR() and rerun.
> 3. Add the CASE logic last.

Common Error:
If you see function date_part(text, text) does not exist, check that your date column is typed as DATE, not TEXT.

6. `dbt run --select prep_weather_daily`
> Running a single model helps focus on one transformation at a time.


Hint: Think about how you can chain multiple CTEs to build features step by step.

<details> 
  <summary>ONLY IF STUCK! Click for semi-filled CTE</summary>

    Use this skeleton and fill in the blanks ... to make it work.

```sql
WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, ... AS date_day 		-- number of the day of month
		, ... AS date_month 	-- number of the month of year
		, ... AS date_year 		-- number of year
		, ... AS cw 			-- number of the week of year
		, ... AS month_name 	-- name of the month
		, ... AS weekday 		-- name of the weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN month_name in ... THEN 'winter'
			WHEN ... THEN 'spring'
            WHEN ... THEN 'summer'
            WHEN ... THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date
``` 

</details> 

-----
## 3. Prep Model: Hourly Weather

Objective: Enhance `staging_weather_hourly` by adding time features like weekday, day, hour, and day part (morning/day/evening/night).

Expected Output Columns


| Column     | Description            |
| ---------- | ---------------------- |
| precipitation_mm | Precipitation (mm) |
| sun_minutes | Sunshine duration |
| temp_c | Temperature celcius |
| snow_mm | Snow (mm) |
| wind_direction | wind direction |
| dewpoint_c | Dewpoint Celcius |
| wind_speed_kmh | wind speed (kmh) |
| pressure_hpa | Pressure |
| timestamp  | Original timestamp     |
| date       | Only the date portion  |
| time       | Only the time portion  |
| hour       | Hour in HH24:MI format |
| month_name | Month name             |
| weekday    | Weekday name           |
| date_day   | Day of month           |
| date_month | Month                  |
| date_year  | Year                   |
| cw         | Calendar week          |
| day_part   | Night, Day, Evening    |


#### Task Instructions

1. Create `prep_weather_hourly.sql` in the prep directory.

2. Reference the staging model using `{{ ref('staging_weather_hourly') }}` .

3.Use CTEs to extract date/time parts using:

- `timestamp::DATE`

- `timestamp::TIME`

- `TO_CHAR(timestamp,'HH24:MI')` -- time (hours:minutes) as TEXT data type

- `DATE_PART('week', timestamp)`

- `TO_CHAR(timestamp, 'FMmonth')` AS month_name   -- month name as a TEXT


4. Use a CASE WHEN statement for day_part intervals:

```sql
CASE
    WHEN time BETWEEN '00:00:00' AND '05:59:00' THEN 'night'
    ...
END AS day_part
```
> If you get “invalid input syntax for type time,” ensure timestamp is actually a TIMESTAMP column.

5. `dbt run --select prep_weather_hourly`


<details> 
  <summary>ONLY IF STUCK! Click for semi-filled CTE</summary>

    Use this skeleton and fill in the blanks ... to make it work.

```sql
WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
		, ... AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , ... AS weekday        -- weekday name as TEXT        
        , DATE_PART('day', timestamp) AS date_day
		, ... AS date_month
		, ... AS date_year
		, ... AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN time BETWEEN ... AND ... THEN 'night'
			WHEN ... THEN 'day'
			WHEN ... THEN 'evening'
		END) AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features
``` 

</details> 

------
## 4. Prep Model: Flights

Objective: Clean staging_flights_one_month:

- Casting integer time columns into TIME

- Creating interval columns for delays

- Converting distances from miles to kilometers

| Column                       | Description                |
| ---------------------------- | -------------------------- |
| flight_date                  | Date of flight             |
| dep_time                     | Departure time (TIME)      |
| sched_dep_time               | Scheduled departure (TIME) |
| dep_delay                    | Minutes                    |
| dep_delay_interval           | Interval type              |
| arr_time                     | Arrival time (TIME)        |
| arr_delay_interval           | Interval type              |
| air_time                     | Flight duration (minutes)  |
| air_time_interval            | Interval type              |
| actual_elapsed_time          | Minutes                    |
| actual_elapsed_time_interval | Interval type              |
| distance_km                  | Distance in kilometers     |


### Task Instructions

1. Create `prep_flights.sql`.

2. Reference the staging model `{{ ref('staging_flights_one_month') }}` .

3. Step 1: Cast all time columns to TIME using TO_CHAR(..., 'fm0000')::TIME.

4. Step 2: Add interval and distance columns:

```sql
(dep_delay * '1 minute'::interval) AS dep_delay_interval
(distance / 0.621371) AS distance_km
```

4.`dbt run --select prep_flights`

<details> 
  <summary>ONLY IF STUCK! Click for semi-filled CTE</summary>

    Use this skeleton and fill in the blanks ... to make it work.

```sql
WITH flights_one_month AS (
    SELECT * 
    FROM {{ref('staging_flights_one_month')}}
),
flights_cleaned AS(
    SELECT flight_date::DATE
            ,TO_CHAR(dep_time, 'fm0000')::TIME AS dep_time
            ,... AS sched_dep_time
            ,dep_delay
		    ,(dep_delay * '1 minute'::INTERVAL) AS dep_delay_interval
            ,...::TIME AS arr_time
            ,...::TIME AS sched_arr_time
            ,arr_delay
            ,(...) AS arr_delay_interval
            ,airline
            ,tail_number
            ,flight_number
            ,origin
            ,dest
            ,air_time
            ,(...) AS air_time_interval
            ,actual_elapsed_time
            ,(...) AS actual_elapsed_time_interval
            ,(distance / 0.621371)::NUMERIC(6,2) AS distance_km -- see instruction hint
            ,cancelled
            ,diverted
    FROM flights_one_month
)
SELECT * FROM flights_cleaned
``` 

</details> 



## Summary

By now, you should see dbt as a **pipeline management tool** — organizing transformations clearly from raw → staging → prep → mart.  
This prepares them for introducing **testing, documentation, and scheduling** in the next phase.

----
# Solutions

1. prep_airports
    <details> 
    <summary>Solution</summary>

    ```sql
    WITH airports_reorder AS (
    SELECT faa
            ,name
            ,city
            ,country
            ,region
            ,lat
            ,lon
            ,alt
            ,tz
            ,dst
    FROM {{ref('staging_airports')}}
    )
    SELECT * FROM airports_reorder
    ``` 

    </details> 


2. prep_weather_daily
    <details> 
    <summary>Solution</summary>

    ```sql
    WITH daily_data AS (
        SELECT * 
        FROM {{ref('staging_weather_daily')}}
    ),
    add_features AS (
        SELECT *
            , DATE_PART('day', date) AS date_day 		-- number of the day of month
            , DATE_PART('month', date) AS date_month 	-- number of the month of year
            , DATE_PART('year', date) AS date_year 		-- number of year
            , DATE_PART('week', date) AS cw 			-- number of the week of year
            , TO_CHAR(date, 'FMmonth') AS month_name 	-- name of the month
            , TO_CHAR(date, 'FMday') AS weekday 		-- name of the weekday
        FROM daily_data 
    ),
    add_more_features AS (
        SELECT *
            , (CASE 
                WHEN month_name in ('december','one_month','february') THEN 'winter'
                WHEN month_name in ('march','april','may') THEN 'spring'
                WHEN month_name in ('june','july','august') THEN 'summer'
                WHEN month_name in ('september','october','november') THEN 'autumn'
            END) AS season
        FROM add_features
    )
    SELECT *
    FROM add_more_features
    ORDER BY date

    ``` 

    </details> 

3. prep_weather_hourly
    <details> 
    <summary>Solution</summary>

    ```sql
    WITH hourly_data AS (
        SELECT * 
        FROM {{ref('staging_weather_hourly')}}
    ),
    add_features AS (
        SELECT *
        , timestamp::DATE AS date -- only date (year-month-day) as DATE data type
        , timestamp::TIME AS time -- only time (hours:minutes:seconds) as TIME data type
            , TO_CHAR(timestamp,'HH24:MI') as hour -- time (hours:minutes) as TEXT data type
            , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
            , TO_CHAR(timestamp, 'FMday') AS weekday        -- weekday name as TEXT            
            , DATE_PART('day', timestamp) AS date_day
            , DATE_PART('month', timestamp) AS date_month
            , DATE_PART('year', timestamp) AS date_year
            , DATE_PART('week', timestamp) AS cw
        FROM hourly_data
    ),
    add_more_features AS (
        SELECT *
            ,(CASE 
                WHEN time BETWEEN '00:00:00' AND '05:59:00' THEN 'night'
                WHEN time BETWEEN '06:00:00' AND '18:00:00' THEN 'day'
                WHEN time BETWEEN '18:00:00' AND '23:59:00' THEN 'evening'
            END) AS day_part
        FROM add_features
    )

    SELECT *
    FROM add_more_features
    ``` 

    </details> 

4. prep_flights
    <details> 
    <summary>Solution</summary>

    ```sql
    WITH flights_one_month AS (
    SELECT * 
    FROM {{ref('staging_flights_one_month')}}
    ),
    flights_cleaned AS(
    SELECT flight_date::DATE
            ,TO_CHAR(dep_time, 'fm0000')::TIME AS dep_time
            ,TO_CHAR(sched_dep_time, 'fm0000')::TIME AS sched_dep_time
            ,dep_delay
            ,(dep_delay * '1 minute'::interval) AS dep_delay_interval
            ,TO_CHAR(arr_time, 'fm0000')::TIME AS arr_time
            ,TO_CHAR(sched_arr_time, 'fm0000')::TIME AS sched_arr_time
            ,arr_delay
            ,(arr_delay * '1 minute'::interval) AS arr_delay_interval
            ,airline
            ,tail_number
            ,flight_number
            ,origin
            ,dest
            ,air_time
            ,(air_time * '1 minute'::INTERVAL) AS air_time_interval
            ,actual_elapsed_time
            ,(actual_elapsed_time * '1 minute'::INTERVAL) AS actual_elapsed_time_interval
            ,(distance / 0.621371)::NUMERIC(6,2) AS distance_km -- see instruction hint
            ,cancelled
            ,diverted
    FROM flights_one_month
    )
    SELECT * FROM flights_cleaned
    ``` 

    </details> 