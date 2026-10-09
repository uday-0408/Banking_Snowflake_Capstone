from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_and_snowflake_test',
    default_args=default_args,
    description='A simple DAG to test dbt and Snowflake connection',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    test_snowflake_connection = SQLExecuteQueryOperator(
        task_id='test_snowflake_connection',
        conn_id='snowflake_default',
        sql='SELECT CURRENT_VERSION();'
    )

    test_dbt_debug = BashOperator(
        task_id='test_dbt_debug',
        bash_command='dbt parse --project-dir /usr/local/airflow/dbt/banking_models --profiles-dir /usr/local/airflow/dbt',
    )

    test_snowflake_connection >> test_dbt_debug
