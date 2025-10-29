#  Schema Tests
----

## The Point of Running Tests in dbt (The “Why”)

In the grand scheme of things, the purpose of running dbt tests is to ensure that your data models are reliable and trustworthy — before they’re used for analysis, dashboards, or machine learning.

### 1.  They validate your transformations, not your code

When you build models (staging → prep → mart), you’re writing SQL that transforms data.
But just because the SQL runs doesn’t mean the data is correct.

For example:

- A missing join condition could duplicate rows.

- A filter might accidentally drop an entire month.

- Null values in key columns might break downstream joins.

- Country codes might contain typos or unexpected values.

- Running dbt tests catches these data quality issues early.

### 2.  They act like “unit tests” for data

Just as software engineers use unit tests to ensure their functions work as expected, data engineers use dbt tests to check that assumptions about the data still hold true.

For example:
```yaml
- name: faa
  tests: [unique, not_null]
```

This says:

> “Every airport FAA code should exist and be unique — if not, something is wrong upstream.”

### 3.  They serve as early warning systems

If a test fails, it tells you exactly which model and column has unexpected data.
That helps prevent:

- Wrong numbers in dashboards

- Broken joins in later models

- Misleading KPIs for stakeholders

In a production pipeline, dbt tests can even stop a deployment if data quality drops.

### 4.  They codify your data assumptions

Each test expresses a business or logical rule about the data — something that’s usually in your head or written in documentation.

By defining it as a dbt test, it becomes executable documentation — always checked, always visible.

For instance:
```yaml
- name: season
  tests:
    - accepted_values:
        values: ['winter', 'spring', 'summer', 'autumn']
```

> This makes it clear (and verifiable) that only those four values are valid — no “wintr” or “Springg”.

### 5.  They ensure your marts are built on solid foundations

By testing your prep layer, you make sure that when you later aggregate or join data in marts, you’re building on clean, valid, consistent inputs.
Otherwise, your KPIs or reports might be completely wrong — and it wouldn’t be obvious why.

----
##  When to Use Schema Tests

You should add the schema tests only after you’ve:

- Created all the prep models (SQL files)

- Successfully run them at least once using the `dbt run --select file` so that the tables (or views) exist in your warehouse/database

> “dbt tests don’t test your code, they test your data.
> So the data has to exist before dbt can check it.”
---- 

## Why?

When you run dbt test, dbt:

- looks up each model in your schema.yml,

- builds a temporary query that checks the condition (e.g., NOT NULL, UNIQUE, ACCEPTED_VALUES), and

- runs it against the actual database table or view.

- If the table doesn’t exist yet — because you haven’t run your model — dbt will throw an error like:
```bash
Database Error in test not_null_prep_airports_faa (models/prep/schema.yml)
relation "analytics.prep_airports" does not exist
```

So the correct sequence is:
| Step | Command                                          | Purpose                                         |
| ---- | ------------------------------------------------ | ----------------------------------------------- |
| 1    | `dbt run --select prep_file`                        | Build the prep models so the tables exist       |
| 2    | `dbt test --select prep_file`                       | Run all the defined schema tests                |

----

## Update your Schema.yml

```yaml
version: 2

models:
  - name: prep_airports
    description: "Reordered airport table for reporting."
    columns:
      - name: faa
        tests: [not_null, unique]
      - name: country
        tests: [not_null]

  - name: prep_weather_daily
    description: "Daily weather data with derived calendar and seasonal features."
    columns:
      - name: date
        tests: [not_null]
      - name: season
        tests:
          - accepted_values:
              values: ['winter', 'spring', 'summer', 'autumn']

  - name: prep_weather_hourly
    description: "Hourly weather data enriched with temporal and categorical features."
    columns:
      - name: timestamp
        tests: [not_null]
      - name: day_part
        tests:
          - accepted_values:
              values: ['night', 'day', 'evening']

  - name: prep_flights
    description: "Cleaned flights data with intervals and distance conversions."
    columns:
      - name: flight_date
        tests: [not_null]
      - name: distance_km
        tests: [not_null]
```

#### Run the dbt test on each of your models.
