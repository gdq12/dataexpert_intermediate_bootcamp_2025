### 1️⃣ Migrate postgres data to Snowflake 

* easiest way:
    + spin up the postgres container from `../setup_postgres` 
    + execute copy commands to export the data into `*csv` files
    + `docker cp` them to a local directory
    + use Snowflake UI to load the data to the target schema

* instructions can be found in [data_postgres_2_snowflake.sql](data_postgrs_2_snowflake.sql)

### 2️⃣ Fetch needed airflow material from teaching repo

* airflow material for lessons are in repo [airlow-dbt-project/dbt_project](https://github.com/DataExpert-io/airflow-dbt-project)

* post the git clone, need to roll back to a previous commit to fetch the pertinent material: `git checkout 8788a8a97049a98445d3b693bb2413a2594d50b2` (Nov 14, 2024 commit)

### 3️⃣ Installing astro CLI

```
brew tap astronomer/tap
# no need for Podman since using docker 
brew install astronomer/tap/astro --without-podman
brew install astro
```

### 4️⃣ Inititating airflow

**to note:** deployment requires `packages.txt`. Just created one here as a place holder

* execute in terminal: `astro dev start`

* post container deployment, can access airflow UI: 

    + http://setup-airflow.localhost:6563 (or as stated in terminal)

    + username/pwd: admin/admin

### 5️⃣ Spin everything down at conclusion of work 

* single command: `astro dev stop`

### Sources:

* [blog](https://hevodata.com/blog/postgres-to-snowflake/) on postgres to snowflake data transfer

* [snowsight](https://docs.snowflake.com/en/user-guide/data-load-web-ui) data loading

* install astro CLI [instructions](https://www.astronomer.io/docs/astro/cli/install-cli)