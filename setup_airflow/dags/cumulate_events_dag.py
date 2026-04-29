import os
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.snowflake_queries import execute_snowflake_query

@dag(
    description="A dag that aggregates data from USER_WEB_EVENTS_DAILY into metrics",
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
def cumulative_web_events_dag():
    upstream_table = f"{os.getenv('SNOWFLAKE_DB')}.{os.getenv('AIRFLOW_SCHEMA')}.USER_WEB_EVENTS_DAILY"
    production_table = f"{os.getenv('SNOWFLAKE_DB')}.{os.getenv('AIRFLOW_SCHEMA')}.USER_WEB_EVENTS_CUMULATED"

    create_step = PythonOperator(
        task_id="create_step",
        python_callable=execute_snowflake_query,
        op_kwargs={
            'query': f"""
             CREATE TRANSIENT TABLE IF NOT EXISTS {production_table} (
                USER_ID NUMBER(38,0),
                DEVICE_ID NUMBER(38,0),
                EVENT_COUNT_ARRAY ARRAY(NUMBER(38,0)),
                EVENT_COUNT_LAST_7D NUMBER(38,0),
                EVENT_COUNT_LIFETIME NUMBER(38,0),
                DS DATE
             ) 
             cluster by (ds)
             """
        }
    )

    yesterday_ds = '{{ yesterday_ds }}'
    ds = '{{ ds }}'
    clear_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_snowflake_query,
        op_kwargs={
            'query': f"""
               DELETE FROM {production_table} 
               WHERE ds = DATE('{ds}')
               """
        }
    )

    cumulate_step = PythonOperator(
        task_id="cumulate_step",
        python_callable=execute_snowflake_query,
        op_kwargs={
            'query': f"""
                 INSERT INTO {production_table}
                 WITH yesterday AS (
                    SELECT 
                        user_id, 
                        device_id, 
                        cast(event_count_array as array) event_count_array,
                        event_count_last_7d, 
                        event_count_lifetime, 
                        ds
                    FROM {production_table}
                    WHERE ds = DATE('{ yesterday_ds }')
                 ),
                 today AS (
                    SELECT 
                        user_id, 
                        device_id, 
                        MAX(event_count) as event_count
                    FROM {upstream_table}
                    WHERE ds = DATE('{ds}')
                    GROUP BY user_id, device_id
                 ),
                 event_arrays AS (
                 SELECT 
                    COALESCE(t.user_id, y.user_id) as user_id,
                    COALESCE(t.device_id, y.device_id) as device_id,
                    CASE 
                        WHEN y.user_id IS NULL THEN array_construct(t.event_count)
                        WHEN t.user_id IS NULL THEN array_prepend(y.event_count_array, 0)
                        ELSE array_prepend(y.event_count_array, t.event_count)
                    END as event_count_array,
                    COALESCE(y.event_count_lifetime,0) as event_count_lifetime
                FROM today t 
                FULL OUTER JOIN yesterday y ON t.user_id = y.user_id 
                                            and t.device_id = y.device_id
                ) 
                SELECT 
                    user_id, 
                    device_id, 
                    event_count_array,
                    array_agg(array_slice(event_count_array, 0, 7))[0][0]::number(38,0) as event_count_last_7d,
                    event_count_lifetime + event_count_array[0] as event_count_lifetime,
                    DATE('{ds}') as ds 
                FROM event_arrays
                group by all    
                 """
        }
    )

    create_step >> clear_step >> cumulate_step


cumulative_web_events_dag()