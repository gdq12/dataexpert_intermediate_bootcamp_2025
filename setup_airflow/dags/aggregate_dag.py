import os
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.snowflake_queries import run_snowflake_query_dq_check, run_snowflake_record_count_check, execute_snowflake_query

@dag(
    description="A dag that aggregates data from postgres events into metrics",
    default_args={
        "owner": "Admin",
        "start_date": datetime(2023, 1, 1),
        "retries": 1,
        "execution_timeout": timedelta(minutes=5),
    },
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    template_searchpath='include/',
    tags=["community"],
)
def aggregate_events_dag():
    upstream_table = f"{os.getenv('SNOWFLAKE_DB')}.{os.getenv('POSTGRES_SCHEMA')}.EVENTS"
    production_table = f"{os.getenv('SNOWFLAKE_DB')}.{os.getenv('AIRFLOW_SCHEMA')}.USER_WEB_EVENTS_DAILY"
    create_step = PythonOperator(
        task_id="create_step",
        python_callable=execute_snowflake_query,
        op_kwargs={
            'query': f"""
             CREATE TRANSIENT TABLE IF NOT EXISTS {production_table} (
                user_id INTEGER,
                device_id INTEGER,
                lesson_page_count INTEGER,
                event_count INTEGER, 
                ds DATE    
            )
            cluster by (ds)
            """
        }
    )
    staging_table = production_table + '_stg'
    create_staging_step = PythonOperator(
        task_id="create_staging_step",
        python_callable=execute_snowflake_query,
        op_kwargs={
            'query': f"""
                 CREATE TRANSIENT TABLE IF NOT EXISTS {staging_table} (
                    user_id INTEGER,
                    device_id INTEGER,
                    lesson_page_count INTEGER,
                    event_count INTEGER, 
                    ds DATE    
                )
                cluster by (ds)
                """
        }
    )

    clear_production_table = PythonOperator(
        task_id="clear_production_table",
        python_callable=execute_snowflake_query,
        op_kwargs={
            'query': """
                        DELETE FROM {production_table}
                        WHERE ds = DATE('{ds}')
                    """.format(production_table=production_table, ds='{{ ds }}')
        }
    )

    clear_staging_table = PythonOperator(
        task_id="clear_staging_table",
        python_callable=execute_snowflake_query,
        op_kwargs={
            'query': """
                        DELETE FROM {staging_table}
                        WHERE ds = DATE('{ds}')
                    """.format(staging_table=staging_table, ds='{{ ds }}')
        }
    )

    load_to_staging_step = PythonOperator(
        task_id="load_to_staging_step",
        python_callable=execute_snowflake_query,
        op_kwargs={
            'query': """
                INSERT INTO {staging_table}
                SELECT 
                    user_id,
                    device_id, 
                    COUNT(CASE WHEN contains(lower(url), 'lessons') THEN 1 END) as lesson_count,
                    COUNT(1) AS event_count,
                    '{ds}' as ds 
                FROM {upstream_table}
                WHERE user_id IS NOT NULL
                AND event_time BETWEEN '{yesterday_ds}' AND '{ds}'
                GROUP BY user_id, device_id 
                """.format(staging_table=staging_table,
                           upstream_table=upstream_table,
                           yesterday_ds='{{ yesterday_ds }}',
                           ds='{{ ds }}')
        }

    )

    run_record_count_check = PythonOperator(
        task_id="run_record_count_check",
        python_callable=run_snowflake_record_count_check,
        op_kwargs={
            'query': """select 
                count(1) row_count
            from {staging_table}
            where date_trunc('day', ds) = '{ds}'
            """.format(staging_table=staging_table,
                       ds='{{ ds }}')
        }
    )

    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_snowflake_query_dq_check,
        op_kwargs={
            'query': f"""
                   SELECT 
                       user_id,
                       COUNT(CASE WHEN event_count = 0 THEN 1 END) = 0 as event_count_should_not_be_zero,
                       COUNT(1) < 5000 AS is_there_too_much_data
                   FROM {staging_table}
                   GROUP BY user_id
               """
        }
    )

    exchange_data_from_staging = PythonOperator(
        task_id="exchange_data_from_staging",
        python_callable=execute_snowflake_query,
        op_kwargs={
            'query': """
                          INSERT INTO {production_table}
                          SELECT * FROM {staging_table} 
                          WHERE ds = DATE('{ds}')
                      """.format(production_table=production_table,
                                 staging_table=staging_table,
                                 ds='{{ ds }}')
        }
    )

    (create_step
     >> create_staging_step
     >> clear_production_table
     >> clear_staging_table
     >> load_to_staging_step
     >> run_record_count_check
     >> run_dq_check
     >> exchange_data_from_staging
     )


aggregate_events_dag()