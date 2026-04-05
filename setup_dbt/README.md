## Step 1️⃣: Setting up Snowflake trial account 

* create a free trial account with $400 credit [here](https://signup.snowflake.com/)

## Step 2️⃣: Set up Docker Image for dev/learning

* Once setup for account complete, create the following vars and store them in [.devcontainer/devcontainer.env](.devcontainer/devcontainer.env):

    ```{bash}
    DBT_ACCOUNT=
    DBT_USER=
    DBT_PWD=
    DBT_WAREHOUSE=
    DBT_ROLE=
    DBT_DB=
    DBT_SCHEMA=
    ```

* snowflake commands for vars are in [snowflake_queries.sql](snowflake_queries.sql)

* Build image and launch container using VS Code 

    ```{bash}
    cd dataexpert_course_material/setup_dbt
    docker build -t dataexpert-dbt:latest .
    ```

## Step 3️⃣: Launch Docker Container

    ```{bash}
    cd dataexpert_course_material/setup_dbt

    source .env
    code .
    ```

## Helpful Sources 

### Env Setup 

* repo for learning material: [dbt-basics](https://github.com/DataExpert-io/dbt-basics) (its been deprecated)

* new location for repo learning material: [airlow-dbt-project/dbt_project](https://github.com/DataExpert-io/airflow-dbt-project/tree/main/dbt_project)

* dbt labs page for [dbt installation](https://docs.getdbt.com/docs/local/install-dbt?version=1.12)

* dbt labs [docker file](https://github.com/dbt-labs/dbt-core/blob/main/docker/Dockerfile) for own customization

* dbt labs [dbt-core repo](https://github.com/dbt-labs/dbt-core): to get the latest releases and tags of the python lib

### Source Data Setup

* [jaffle-shop-generator](https://github.com/dbt-labs/jaffle-shop-generator): to create synthetic data based on jaffle shop 

* [s3 jaffle data links](https://github.com/dbt-labs/jaffle-shop-generator) from another dbt project 

* [dbt labs jaffle shop project](https://github.com/dbt-labs/jaffle-shop-classic), which has been depracated

* [snowsight instructions](https://docs.snowflake.com/en/user-guide/data-load-web-ui) for loading data to snowflake

### Other good to knows

* [dbt_project_evaluator](https://hub.getdbt.com/dbt-labs/dbt_project_evaluator/latest/): does a sanity checkon how well the current state of the project aligns with dbt labs best practices

* [db-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint): another good project evaluator which goes into more detail testing on how well the project stands 