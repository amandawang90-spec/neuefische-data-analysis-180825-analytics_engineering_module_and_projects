# Introduction to dbt

## What is dbt?

dbt (data build tool) is a tool that makes it easier to build and maintain data pipelines and data warehouses.
It has become popular because it bridges the gap between data engineering and data analytics, helping the rise of analytics engineering as a role.

There are two main approaches to integrating data in pipelines:

#### ETL (Extract, Transform, Load):

- Extract data from the source.

- Transform the data.

- Load it into the data warehouse.

#### ELT (Extract, Load, Transform):

- Extract data from the source.

- Load it into the data warehouse.

- Transform it inside the warehouse.

> In ELT, data arrives in the warehouse in its raw form. dbt is commonly used for the transform step in ELT because it allows transformations to be testable, version-controlled, and well-documented.

-------

## Why use dbt?

dbt offers many advantages for building and maintaining data pipelines:

- Open source and free to use

- Easy to start and use immediately

- Simplifies testing of models

- Enables thorough documentation

- Supports version control (works well with Git)

- Easy deployment to multiple environments

- Can be automated and scheduled

- Integrates smoothly with other tools

- Easy to maintain over time

------

## How does dbt work?

dbt adds a modeling layer on top of your data warehouse.

- Each dbt model is a .sql file.

- Models can depend on raw tables or other models.

- The output is stored as views or tables in your data warehouse.

A typical dbt project follows this structure:

1. Raw data – tables imported from sources (e.g., Meteostat JSON).

2. Staging models – clean data (rename columns, remove duplicates).

3. Prep models – aggregated data (sum, count, averages).

4. Analysis / Mart models – final tables for stakeholders (marketing, sales, etc.).

> Staging and prep models are modular, making the data pipeline easier to manage, while marts provide stakeholder-ready tables.

------

## dbt Project Structure

All dbt code should be version-controlled in a GitHub repository. A standard dbt repository includes:

- dbt_project.yml – project definition and global settings

- dbt_packages/ – optional external packages

- logs/ – logs of dbt runs

- macros/ – reusable SQL code (like functions)

- models/

    - staging/ – initial modular building blocks from raw data

    - prep/ – intermediate transformations

    - marts/ – stakeholder-facing final tables

- seeds/ – CSV files that dbt can load into the warehouse

- snapshots/ – historical tracking of mutable tables

- target/ – compiled SQL code

- tests/ – assertions for model and source validation


------------

## dbt Core vs dbt Cloud

- dbt Core – open-source, free, but requires local setup and technical knowledge.

- dbt Cloud – commercial, beginner-friendly, includes job orchestration and a web interface.

For this week, we will use dbt Cloud to simplify onboarding.