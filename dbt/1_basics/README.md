## Getting started with the lab 

1. spin up a docker container based on image set up in [setup_dbt](../../setup_dbt)

    ```{bash}
    cd dataexpert_course_material/setup_dbt

    source .env
    code .
    ```
 2. test out everything all good with dbt setup via `dbt debug`

## Notes on project setup 

* command: `dbt debug`

    - it checks on the following config are all good: `profiles.yml`, `dbt_project.yml`, git setup and can connect to configed DB (in this case Snowflake)

* file: `profiles.yml`

    - this is essentially the connection config file 

    - it tells dbt what to connect to (prod/dev), what account credentials to use for connex, what location (db.schema) should be working in etc

* file: `dbt_project.yml`

    - it is the central config file for the whole project 

    - can auto config model materialization type, documentation configs (like node colors)

    - define env vars during project runs here (those typically used by macros)

    - the config overwritting goes as follows: `model >> layer >> project`

* to generate documentation: `dbt docs generate` --> `dbt docs serve`

    - can see full project docs here

    - can also visualize lineage, which is helpful for larger and more complex projects 

* command: `dbt deps` --> to install packages configed in `packages.yml`

* file: `_sources.yml`

    - this config file defines the model data sources, aka data that is extracted and loaded for transformation by other mechanisms tools (airflow, step fucntions etc.)

    - its well advised to keep this file in the same model directory as the stage models since stage models source the data from these tbls 

    - typically name and schema are the same value to reduce confusion 

    - dbt codegen library can help a lot with generating documentation. can visit the [repo](https://github.com/dbt-labs/dbt-codegen) page to get more details on the syntax

        + ex: `dbt run-operation generate_source --args '{"schema_name": env_var('DBT_SCHEMA'), "database_name": env_var('DBT_DB'), "table_names": ["js_raw_orders", "js_raw_payments", "js_raw_customers"]}'`

        + fyi models needs to be already built in snowflake to generate the docs

* referencing sources and models 

    - `{{ source('name', 'tbl_name') }}`: referencing a model that **isn't** transformed/managed by dbt

    - `{{ ref('model_name') }}`: referencing a model that **is** transformed/managed by dbt 

* command: `dbt build -s modelName --debug`

    - `--debug` flag is very handy when trying to figure out what is causing the error messages

    - provides a bit more detail on whether is a syntax or permissions error. Doesn't explicitely state it but can determine from context clues 

* directory: `target`

    - here can find the logs and compiled queries sent to dbt post comoilation during a run/build

    - `target/compiled` are just files of the compiled code from `models` --> select statements

    - `target/run` consists of files where the compiled code is wrapped into create table/view --> create statements

* command: `dbt seed`

    - it permits the user to load a **small** CSV file from the `seed/` as a table in the DB 

    - the tbl generation configuration can be done in `dbt_project.yml` or in a config file in the same directory 