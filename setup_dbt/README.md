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

* repo for learning material: [dbt-basics](https://github.com/DataExpert-io/dbt-basics)

* dbt labs page for [dbt installation](https://docs.getdbt.com/docs/local/install-dbt?version=1.12)

* dbt labs [docker file](https://github.com/dbt-labs/dbt-core/blob/main/docker/Dockerfile) for own customization

* dbt labs [dbt-core repo](https://github.com/dbt-labs/dbt-core): to get the latest releases and tags of the python lib

### Source Data Setup

* [jaffle-shop-generator](https://github.com/dbt-labs/jaffle-shop-generator): to create synthetic data based on jaffle shop 

* [s3 jaffle data links](https://github.com/dbt-labs/jaffle-shop-generator) from another dbt project 

* [dbt labs jaffle shop project](https://github.com/dbt-labs/jaffle-shop-classic), which has been depracated

* [snowsight instructions](https://docs.snowflake.com/en/user-guide/data-load-web-ui) for loading data to snowflake