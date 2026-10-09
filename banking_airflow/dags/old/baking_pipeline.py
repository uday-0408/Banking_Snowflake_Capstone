# pyrefly: ignore [missing-import]
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import os


SNOWFLAKE_CONN_ID = "snowflake_conn"

LOCAL_FOLDER = "/usr/local/airflow/data/raw"

STAGE = "@bronze.banking_stg"


def upload_files_to_stage():

    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:

        files = [
            file_name
            for file_name in os.listdir(LOCAL_FOLDER)
            if file_name.endswith(".csv")
        ]

        if not files:
            print("No CSV files found.")
            return

        for file_name in files:

            local_path = os.path.join(
                LOCAL_FOLDER,
                file_name
            )

            print(f"Uploading: {local_path}")

            sql = f"""
                PUT 'file://{local_path}'
                {STAGE}
                AUTO_COMPRESS=FALSE
                OVERWRITE=FALSE;
            """

            cursor.execute(sql)

            result = cursor.fetchall()

            print(f"PUT result for {file_name}:")
            print(result)

    finally:
        cursor.close()
        conn.close()

def load_stage_to_bronze():

    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    sql = """
        COPY INTO bronze.banking_raw
        FROM (
            SELECT
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11,
                $12,
                $13,
                $14,
                $15,
                $16,
                $17,
                $18,
                $19,
                $20,
                $21,
                $22,
                $23,
                $24,
                $25,
                $26,
                $27,
                $28,
                $29,
                $30,
                $31,
                $32,
                $33,
                $34,
                $35,
                $36,
                $37,
                $38,
                $39,
                $40,
                $41,
                $42,
                $43,
                $44,
                $45,
                METADATA$FILENAME,
                CURRENT_TIMESTAMP()
            FROM @bronze.banking_stg
        )
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        );
    """

    print("Loading files from stage into bronze.banking_raw...")

    result = hook.run(sql)

    print("COPY INTO completed.")
    print(result)


with DAG(
    dag_id="banking_pipeline",

    start_date=datetime(2026, 9, 18),

    schedule="@daily",

    catchup=False,

    tags=["banking", "snowflake", "bronze"],

) as dag:

    upload_to_stage = PythonOperator(
        task_id="upload_files_to_stage",
        python_callable=upload_files_to_stage,
    )

    copy_into_bronze = PythonOperator(
        task_id="copy_stage_to_bronze",
        python_callable=load_stage_to_bronze,
    )

    run_banking_clean = BashOperator(
        task_id="run_banking_clean",
        bash_command="""
            cd /usr/local/airflow/dbt &&
            dbt build --select banking_clean
        """,
    )

    run_dbt_snapshot = BashOperator(
        task_id="run_dbt_snapshot",
        bash_command="""
            cd /usr/local/airflow/dbt &&
            dbt snapshot
        """,
    )

    run_banking_gold = BashOperator(
        task_id="run_banking_gold",
        bash_command="""
            cd /usr/local/airflow/dbt &&
            dbt run --select gold
        """,
    )

    upload_to_stage >> copy_into_bronze >> run_banking_clean >> run_dbt_snapshot >> run_banking_gold