from pathlib import Path
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task

DATA_DIR = Path("/usr/local/airflow/data/raw")
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_STAGE = "CAPSTONE_EXAM.PRACTICE.PRAC_STAGE"

@task
def upload_files_to_stage():
    # 1. Find CSV files inside the mounted directory.
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise AirflowException(f"No CSV files found in {DATA_DIR}")

    # 2. Open the Snowflake connection.
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    connection = hook.get_conn()
    cursor = connection.cursor()

    try:
        # 3. Upload every discovered CSV file.
        for file_path in csv_files:
            file_uri = f"file://{file_path.resolve().as_posix()}"
            print("File url: ", file_uri) 

            put_sql = (
                f"PUT '{file_uri}' "
                f"@{SNOWFLAKE_STAGE} "
                "AUTO_COMPRESS=TRUE "
                "OVERWRITE=FALSE"
            )

            cursor.execute(put_sql)
            result = cursor.fetchall()
            print(f"Upload result for {file_path.name}: {result}")
    finally:
        cursor.close()
        connection.close()
