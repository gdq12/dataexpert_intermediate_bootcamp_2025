# Airflow Fundamentals with SCDs Homework

### Query Homework


Assignment
==================

## Dataset Overview

This assignment involves working with the **`bootcamp.actor_films`** dataset. Your task is to construct a series of SQL queries and table definitions that will allow us to model the **`actor_films`** dataset in a way that facilitates efficient analysis. This involves creating new tables, defining data types, and writing queries to populate these tables with data from the **`actor_films`** dataset.

The `bootcamp.actor_films` dataset contains the following fields:

- `actor`: The name of the actor.
- `actor_id`: A unique identifier for each actor.
- `film`: The name of the film.
- `year`: The year the film was released.
- `votes`: The number of votes the film received.
- `rating`: The rating of the film.
- `film_id`: A unique identifier for each film.

The primary key for this dataset is (`actor_id`, `film_id`).

## Assignment Tasks

### Actors History SCD Table DDL (query_1.sql)

Write a DDL statement to create an `<your_username>.actors_history_scd` table that tracks the following fields for each actor in the `actor_films` table:

- `is_active`
- `start_date`
- `end_date`

Note that this table should be appropriately modeled as a Type 2 Slowly Changing Dimension Table (`start_date` and `end_date`).

### Actors History SCD Table Incremental Backfill Query (query_2.sql)

Write an "incremental" query that can populate a single year's worth of the `actors_history_scd` table by combining the previous year's SCD data with the new incoming data from the `actors` table for this year.

### Airflow Dag (dag_1.py)
- Build a DAG that processes the **bootcamp.actor_films** data into **<your_username>.actor_history_scd**

This DAG should follow all cumulative DAG best practices such as:

- It needs to have depends_on_past set to True
- It needs all the valid partition sensors
- It should process and create the SCD incrementally
- It should use {{ ds }} macros correctly for filtering `year` in `bootcamp.actor_films`
- It should be a `@yearly` cadence DAG

### How to submit

- Take `query_1.sql`, `query_2.sql` and `dag_1.py` and put them in a zip file
- Go to the [Assignments page](https://www.dataexpert.io/assignments) and upload it there

## Sources
* fetched from [analytics-engineering-bootcamp-homework](https://github.com/DataExpert-io/analytics-engineering-bootcamp-homework/blob/main/airflow-fundamentals/homework.md) repo