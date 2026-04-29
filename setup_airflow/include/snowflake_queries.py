import os 
import snowflake.connector
from snowflake.snowpark import Session

connection_params = {
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "user": os.getenv('AIRFLOW_USER'),
    "password": os.getenv('AIRFLOW_PWD'),
    "role": os.getenv('AIRFLOW_ROLE'),
    'warehouse': os.getenv('AIRFLOW_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DB')
}

def run_snowflake_record_count_check(query):
    results = execute_snowflake_query(query)
    if len(results) == 0:
        raise ValueError('The query returned no results!')
    for rec in results:
        if rec == 0:
            raise ValueError('No records found in stg for the day')
        else:
            print('{rec} records found in stg')

def run_snowflake_query_dq_check(query):
    results = execute_snowflake_query(query)
    if len(results) == 0:
        raise ValueError('The query returned no results!')
    for result in results:
        for column in result:
            if type(column) is bool:
                assert column is True


def execute_snowflake_query(query):
    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(**connection_params)
    try:
        # Create a cursor object to execute queries
        cursor = conn.cursor()
        # Example query: Get the current date from Snowflake
        cursor.execute(query)
        # Fetch and print the result
        result = cursor.fetchall()
        return result
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()