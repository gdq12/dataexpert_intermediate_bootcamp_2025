## Getting started with the lab

* using [setup_airflow](../../setup_airflow/) for lesson 

* initiate airflow containers: `astro dev start`

### Aggregate dag

* refers to the dag defined in [aggregate_dag.py](../../setup_airflow/dags/aggregate_dag.py)

* It is adopted a bit from the lesson dag. Since there is no access to the kafka data, just manually copied data from the [setup_postgres](../../setup_postgres/) material to snowflake

* this script does a form of WAP (write-audit-publish)

* how WAP is implemented via the tasks:

    - create_step: create a production table if it doesnt already exists 
    - create_staging_step: creates a staging tbl if not already exists 
    - clear_production_table: removes records from the production tbl of a particular partition (those of the execution-date)
    - clear_staging_table: removes records from the stage tbl of a particular partition (those of the execution-date)
    - load_to_staging_step: loads stage tbl of aggregate records, **write part**
    - run_record_count_check: runs a record count check to verify there are records in stg for the target date
    - run_dq_check: runs a test query to verify the records collected within stage meet some assumptions, ideally the boolean conditions should come out true, **audit part**
    - exchange_data_from_staging: inserts the stg records into the production tbl, **publish part**

* the pipeline is made idempotent by always running the pipeline on a slice of data, aka on specific event days (from yesterday to today)

### Cumulative event dag

* refers to the dag defined in [cummulate_events_dag.py](../../setup_airflow/dags/cumulate_events_dag.py)

* its down stream to the aggregate dag 

* its a typical cumulative tbl. Based on `user_id` and `device_id` it compiles an array to store number of web hits the user did on a platform (accounting for gaps days)

* this DAG was a bit simpler in that it doesn't employ WAP, simply collects the data and then inserts it into the production tbl