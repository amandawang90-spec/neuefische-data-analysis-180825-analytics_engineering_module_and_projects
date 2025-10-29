# Lesson: Building Data Marts with dbt
---
## 1. Review: The Pipeline

Before we dive into data marts, let’s quickly review the full data pipeline you’ve built so far:

``` bash
raw → staging → prep → marts
        ↑         ↑      ↑
   cleaned     enriched  aggregated (business-ready)
```

- Staging models – Light cleaning, type casting, renaming, and filtering of source data.

- Prep models – Enrichment: adding derived columns, calculations, and transformations.

- Data Marts – Aggregated, business-focused tables that directly answer stakeholder questions.

---
## 2. What are Data Marts?

Data marts are subject-focused subsets of a data warehouse — e.g., one for flights, another for weather.
They make insights more accessible to specific teams or departments.

There are two main types of mart tables:

| Type                | Represents                     | Example  | Analogy                     |
| ------------------- | ------------------------------ | -------- | --------------------------- |
| **Fact table**      | Events or transactions (verbs) | Flights  | “What happened?”            |
| **Dimension table** | Entities (nouns)               | Airports | “Who or what was involved?” |


In this project:

- Facts → flights, routes

- Dimensions → airports, weather

- Marts → aggregated summaries (KPIs)

![stages](./images/data_marts.png)


---

## 3. Brainstorm: Stakeholder Requirements

Before modeling, let’s think like stakeholders.

Ask yourself:

- What KPIs would decision-makers care about?

- What kind of visualizations would help them?

Example stakeholder questions:

- “Which airports experience the most flight cancellations?”

- “Do delays happen more during bad weather?”

- “How do day vs. night flights differ?”

- “What are the busiest flight routes?”

Task:
Write down 2–3 tables you could build to answer these questions.


---
## 4. General Instructions for Data Marts

Each mart:

- Should reference prep models (never source() directly)

- Should aggregate or summarize data (e.g., COUNT, AVG, MIN, MAX)

- Should be documented in a schema.yml

Example schema.yml entry:

```yaml
version: 2
models:
  - name: mart_airport_stats
    description: "Aggregated airport-level flight statistics"
    columns:
      - name: airport_code
        description: "IATA airport code"
      - name: total_flights
        description: "Total number of flights (departures + arrivals)"

```
> Tip:
Use ref() to link dbt models:

> FROM {{ ref('prep_flights') }}


> Never use source() inside marts — it breaks lineage and dependencies.

Running dbt Commands

Run a specific mart:

`dbt run --select file`


Test all marts:

`dbt test --select file`

----

# 5. Data Marts to Create
---

## 5.1 Airport Statistics – mart_faa_stats.sql
Objective:

Create an airport-level summary with total flights, cancellations, diversions, and unique connections.
We want to see for each airport over all time:

- unique number of departures connections

- unique number of arrival connections

- how many flight were planned in total (departures & arrivals)

- how many flights were canceled in total (departures & arrivals)

- how many flights were diverted in total (departures & arrivals)

- how many flights actually occured in total (departures & arrivals)

- (optional) how many unique airplanes travelled on average

- (optional) how many unique airlines were in service on average

- add city, country and name of the airport

#### Dependencies

- prep_flights

- prep_airports

#### Core Metrics

- Unique destinations per airport (departures)

- Unique origins per airport (arrivals)

- Total planned flights

- Total cancelled, diverted, and completed flights

#### Optional Metrics

- Unique aircraft (tails)

- Unique airlines

<details> <summary>Hint (click to expand)</summary>


- Use two subqueries (departures and arrivals).

- Join them on the airport code (faa).

- Finally, join airport metadata from prep_airports.
</details>

#### Expected Output

Each row = one airport
Columns include flight totals and airport details (city, country, name).

#### Checkpoint:

Does each airport appear once?

Are your totals logical (no negative values)?

Do airports with more flights have higher diversion counts?

----

## 5.2 Flight Route Statistics – mart_route_stats.sql

Objective

Aggregate flights by route (origin → destination).

We want to see for each route over all time:

- origin airport code
- destination airport code
- total flights on this route
- unique airplanes
- unique airlines
- on average what is the actual elapsed time
- on average what is the delay on arrival
- what was the max delay?
- what was the min delay?
- total number of cancelled
- total number of diverted
- add city, country and name for both, origin and destination, airports


#### Dependencies

- prep_flights

- prep_airports

#### Core Metrics

- Total flights

- Unique aircraft and airlines

- Average, min, max arrival delay

- Total cancelled and diverted flights

<details> <summary>Hint (click to expand)</summary>

- Group by (origin, dest)

- Join prep_airports twice (for origin and destination metadata)

- Use meaningful aliases (origin_city, dest_city)

</details>

#### Why cast to intervals?

- We convert numeric times (in minutes) into readable intervals:

- AVG(actual_elapsed_time)::INTEGER * ('1 second'::INTERVAL) AS avg_actual_elapsed_time

## Checkpoint:

- Do you have one row per route?

- Does your join correctly show city and airport names?

---- 
## 5.3 Route Statistics + Weather – mart_selected_faa_stats_weather.sql
Objective

Combine flight and weather data to understand how conditions affect flight operations.

We want to see **for each airport daily**:

- only the airports we collected the weather data for
- unique number of departures connections
- unique number of arrival connections
- how many flight were planned in total (departures & arrivals)
- how many flights were canceled in total (departures & arrivals)
- how many flights were diverted in total (departures & arrivals)
- how many flights actually occured in total (departures & arrivals)
- *(optional) how many unique airplanes travelled on average*
- *(optional) how many unique airlines were in service  on average* 
- (optional) add city, country and name of the airport
- daily min temperature
- daily max temperature
- daily precipitation 
- daily snow fall
- daily average wind direction 
- daily average wind speed
- daily wnd peakgust


#### Dependencies

- prep_airports

- prep_flights

- prep_weather_daily

#### Core Metrics

- Same flight metrics as airport stats

- Daily weather: temperature, precipitation, wind, snow

<details> <summary>Hint (click to expand)</summary>

- Join prep_flights with prep_weather_daily using:

`faa = airport_code AND flight_date = date`

- Limit to airports that appear in prep_weather_daily

- Optional metrics: average temperature, percent cancelled, or wind impact ratio.

</details>

#### Checkpoint:

- Do you see one row per airport per day?

- Does every airport have weather values (if available)?

-----
## 5.4 Weekly Weather Aggregates – mart_weather_weekly.sql

Objective

Roll up daily weather into weekly summaries.

- consider whether the metric should be Average, Maximum, Minimum, Sum or [Mode](https://wiki.postgresql.org/wiki/Aggregate_Mode)

#### Dependencies

- prep_weather_daily

#### Metric choices matter:

- Temperature: average

- Precipitation: sum

- Wind gusts: max

- Snow: sum

<details> <summary>Hint (click to expand)</summary>

Use `DATE_TRUNC('week', date)` to group by week.
``` sql
SELECT DATE_TRUNC('week', date) AS week_start
     , AVG(avg_temp_c) AS avg_temp_c
     , MAX(max_temp_c) AS max_temp_c
     , SUM(precipitation_mm) AS total_precipitation_mm
FROM {{ ref('prep_weather_daily') }}
GROUP BY week_start
```
</details>


#### Checkpoint:

- Is your week_start always a Monday (or consistent)?

- Are all numeric values realistic (no negatives for precipitation)?

---
## 5.5 Optional Custom Marts

Possible ideas:

- Seasonal performance: average delay by season.

- Weather correlation: flights delayed vs. precipitation.

- Comfort index: calculate a “sunny day score.”

Reminder:

- Reference only prep_ models

- Document columns in schema.yml

- Run incrementally (`dbt run --select my_model`)

----

## 6. Common Issues and Debug Tips

| Error                     | Likely Cause                     | Fix                                  |
| ------------------------- | -------------------------------- | ------------------------------------ |
| `relation does not exist` | Dependent model not run          | Run `dbt run --select +model_name`   |
| `column does not exist`   | Typo or wrong join alias         | Double-check SELECT and JOIN clauses |
| `duplicate column name`   | Same name after joins            | Use `AS` aliases                     |
| Empty table               | Filter or join excludes all data | Check WHERE conditions and join keys |


----
## 7. Recommended Order of Execution

Always follow model dependencies:

1. prep_airports
2. prep_flights
3. prep_weather_daily
4. mart_faa_stats
5. mart_route_stats
6. mart_selected_faa_stats_weather
7. mart_weather_weekly
8. (Optional) Custom marts

---
# Key Takeaways

- Marts are business-facing, not raw data tables

- Build on prep models for consistent lineage

- Document every mart in schema.yml

- Test models with dbt test to validate data quality

- Logical checks (e.g., positive values, consistent joins) are just as important as SQL syntax