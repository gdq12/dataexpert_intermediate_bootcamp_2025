## Getting started with the lab 

1. spin up a docker container based on image set up in [setup_dbt](../../setup_dbt)

    ```{bash}
    cd dataexpert_course_material/setup_dbt

    source .env
    code .
    ```
 2. test out everything all good with dbt setup via `dbt debug`

 ## Lesson Themes 

 ### Source data freshness 

 * test how up to date source data is 

 * this is defined in [_sources.yml](../../setup_dbt/models/staging/_sources.yml): warning if `max(last_updated_dt)` is more than 1 day from current run and throws an error when its more than 7 days from current run 

 * it is tested using the following command: `dbt source freshness`

 * the compiled query sent to snowflake: 

    ```{SQL}
    select
        max(cast(last_updated_dt as timestamp)) as max_loaded_at,
        convert_timezone('UTC', current_timestamp()) as snapshotted_at
        from DATAEXPERT.DBT_BASIC.js_raw_customers
    /* {"app": "dbt", "dbt_version": "1.11.7", "profile_name": "jaffle_shop", "target_name": "dev", "node_id": "source.jaffle_shop.bootcamp.js_raw_customers"} */
    ```

* according to [dbt labs docs](https://docs.getdbt.com/docs/deploy/source-freshness?version=1.12) source freshness is not included in `dbt build` this must be configured in dbt fusion or airflow dag

### Configs on model materialization

* by default dbt sets model creation as views

* methods of modifying this 

    - config block: 

        ```
        {{ config(
            materialized='table'
            ) 
        }}
        ```

        + added at the top of the model `*.sql` file 

        + this overrides any configs set at the project or schema level 

    - project level 

        ```
        models:
            jaffle_shop:
                +materialized: table
                staging:
                    +materialized: view
        ```

        + can config this in `dbt_profile.yml` (the project level) to set the materialization method at ever transformation level 

        + the congig syntax is identified with a `+`

    - property/model schema file 

        ```
        models:
            - name: stg_orders 
              config:
                materialized: table
        ```

* details on all different types of model configurations can be found in [this](https://docs.getdbt.com/reference/model-configs?version=1.12) doc page

* best practice is:

    - when all models in a given transformation layer should be **materialized in the same method**, then best in `dbt_project.yml`

    - when specific models within a transformation layer need **custom/different materialization**, then best as a config block or the property file

### dbt data - tests

* command: `dbt test`

    - by default with that command will cimply run all tests in project 

    - to run for only specific models: `dbt test -s ModelName`

* test are always executed for `dbt build` --> data-tests are always run after model compilation

* generic data tests 

    - these are tests that can be reused by any model in the project

    - when a test fails, the mesage indicates how many records were found: `[FAIL #]`

    ```
    models:
        - name: stg_payments
            description: staging table for orders
            columns:
            - name: id
                data_tests:
                - unique
                - not_null
            - name: order_id
                data_tests:
                    - not_null
                    - relationships:
                        to: ref('stg_orders')
                        field: id
            - name: payment_method
                data_tests:
                    - accepted_values:
                        values:
                            - coupon
                            - credit_card
                            - bank_transfer
                            - gift_card
            - name: amount
                data_tests:
                    - not_null
    ```

    - they are applied on columns of a model that are configured within the model's property file 

    - to test for primary key, need to use `unique` and `not_null` tests 

    - prior to dbt version `1.8`, there were no unit test, so `tests:` instead of `data_tests:` was used. When using version `1.9+` need to specify `data_tests:`

    - `accepted_values` test are good to make sure that have no surprise records within the model, aka it is known what all possible values are that are coming from source or a case statement is configured correctly 

    - `relationship` test is good to make sure there is a good primary - foreign key relationship between 2 models. An entity or transactional event key exists across the needed models 

* creating custom generic data tests

    - must be stored in `data_test/generic/` and save each test as an SQL (`*.sql`)

    - need to write tests that will produce faulty records 

    - for a custom generic test, the inputes are always: `model` and `column_name`

    - good examples of these test are [is_even.sql](../../setup_dbt/data_test/generic/is_even.sql) and [is_odd.sql](../../setup_dbt/data_test/generic/is_odd.sql)

    - they can be applied to a model just like out of the box generic tests (within a model's propoerty file)

* creating singular tests

    - they are custom tests written for specific models and columns 

    - in this instance they are created in `data_tests/`, but they can in fact be created within any direcotry of the project

    - the goal of these test are to also return faulty records 

    - since the model is reference already within the test, the test doesnt need to be referenced in the model's property file

    - will be run by `dbt test`/`dbt test -s ModelName` as well 

    - an example of this implementation is [assert_total_payment_amount](../../setup_dbt/data_tests/assert_total_payment_amount.sql)

* other tests from dbt community 

    - apart from `dbt_utils`, there are other packages that come with tests

    - one of the most famous packages is `dbt_expectations`

    - an example of test from this pacakage being implemented is for model [fact_orders](../../setup_dbt/models/mart/fact_orders.yml), where the test `dbt_expectations.expect_column_values_to_be_between` is applied to the amount column

### dbt unit - tests

* they are test that want to test the logic of a model, usually logic that is quite complex and already have in mind what the ideal records should be 

* they are defined in the propoerty file of a given model

* the expected output (for the testing comparison) can be defined as a dict, csv or sql statments

* an example of this is for the [agg_orders](../../setup_dbt/models/marts/agg_orders.sql) model 

* they can be run with the `dbt test -s ModelName` command, if the test were to fail (records in the model to not match entries of the expected output) the fail message would render to specific records where the test pass vs failed

* when a model has both unit and data-tests --> unit test are first run (the actual model isn't needed) --> unit test passes --> builds/updates the model --> runs the data-tests

* how the unit test query is being compiled and tested:

    - creates records that emulate the first CTE which fetches records from the upstream model (`fact_orders`), by using the entries hard coded from the input section of the unit tests

    - takes those records and performs the aggregtion of the 2nd CTE using the records from the previous point

    - does the same as the 2 previous point but for the records of the hard coded expected records of the test

    - extracts the results from the DB and hypothesize it performs a pandas pivot func to compare the expected vs actual per group by 

* based on the sql compilation analysis above, the priority of this unit test is to verify that the aggregation performance is being performed correctly, that is why data from upstream/source models aren't used

### Snapshots 

* creates an SCD2 like table in the DB for the target dim table 

* its best to use source table to best create records that reflect changes in metadata data of the entity

* in general, these models can't be rebuilt. best to have different ownership privilages for this model compared to others

* 2 methods to defining snapshots

    - using `*.sql`: 

        + this is the pre dbt `v1.9`

        + similiar to the other method except the location is completely determined by the schema config --> can lead to incorrect filling or accidential deleting of the model all together if ppl unaware of their use 

    - using `*.yml`

        + this was introduced in dbt `v1.9+`

        * it is now the preferred method to create the SCD2 models 

        * it is less flexible in terms of schema --> schema is either `snapshots` or schema_name + snapshots --> therefor on the first run it will try to create a schema if a schema of either option isn't found 

* whats being executed by dbt under the hood:

    * creates a temp table that compares records from the source table vs the current snapshot table and compares records based on `id` and the `dbt_valid_from`/`last_updated_dt` to determine which `id` are new or updated from source

    * then it uses a merge statment to update/insert records into the snapshot table based on `id`, using conditions `when match` and `when not match` to merge accordingly


## To note

 * for the source table `dataexpert.dbt_basics.js_raw_customers`, the dimensions `country_code` and `last_update_dt` were manually added to the csv prior to loading it to snowflake. These 2 dimensions are completely fabricated, not sourced from the original [dbt labs jaffle shop project](https://github.com/dbt-labs/jaffle-shop-classic) repo

 * its good to specify the model configs within `dbt_project.yml` to a specific project in order to prevent the configs from being applied to models to directories apart from `models/`. For instance, in `dbt_packages/` there are models defined there to carry out testing etc. The configs of the projects should be kept as is and shouldn't be changed by the project config. So it should be kept as so in this instance:

    ```
    name: 'jaffle_shop'

    config-version: 2
    version: '0.1'

    profile: 'jaffle_shop'

    model-paths: ["models"]
    seed-paths: ["seeds"]
    test-paths: ["tests", "data-tests"]
    analysis-paths: ["analysis"]
    macro-paths: ["macros"]

    target-path: "target"
    clean-targets:
        - "target"
        - "dbt_modules"
        - "logs"

    require-dbt-version: [">=1.0.0", "<2.0.0"]

    models:
    jaffle_shop:
        +materialized: table
        staging:
            +materialized: view
    ```

* `dbt build` = run + test + seed + snapshot

* a homework does exist for the dbt-basics section, [dbt_basics/homework](https://github.com/DataExpert-io/analytics-engineering-bootcamp-homework/tree/main/dbt_basics/homework) but the data isn't made available to complete the assignment