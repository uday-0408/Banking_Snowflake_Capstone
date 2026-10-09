from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator

from practice.stage_operations import upload_files_to_stage

SNOWFLAKE_CONN_ID = "snowflake_default"

# Define the Bronze loading SQL
CREATE_BRONZE_SQL = """
CREATE SCHEMA IF NOT EXISTS CAPSTONE_EXAM.BRONZE;

CREATE TABLE IF NOT EXISTS CAPSTONE_EXAM.BRONZE.banking_raw (
    transaction_id VARCHAR, transaction_date VARCHAR, transaction_type VARCHAR, amount VARCHAR,
    currency VARCHAR, transaction_status VARCHAR, payment_channel VARCHAR, payment_method VARCHAR,
    running_balance VARCHAR, is_flagged_fraud VARCHAR, merchant_name VARCHAR, merchant_category VARCHAR,
    customer_id VARCHAR, customer_name VARCHAR, customer_dob VARCHAR, customer_gender VARCHAR,
    customer_email VARCHAR, customer_phone VARCHAR, customer_city VARCHAR, customer_country VARCHAR,
    customer_segment VARCHAR, kyc_status VARCHAR, customer_annual_income VARCHAR, credit_score VARCHAR,
    account_id VARCHAR, account_type VARCHAR, account_open_date VARCHAR, account_status VARCHAR,
    branch_id VARCHAR, branch_name VARCHAR, credit_card_id VARCHAR, card_type VARCHAR,
    card_network VARCHAR, credit_limit VARCHAR, card_status VARCHAR, loan_id VARCHAR,
    loan_type VARCHAR, loan_amount VARCHAR, interest_rate VARCHAR, tenure_months VARCHAR,
    loan_status VARCHAR, complaint_id VARCHAR, complaint_category VARCHAR, complaint_priority VARCHAR,
    complaint_status VARCHAR, source_file_name VARCHAR, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

COPY_INTO_BRONZE_SQL = """
COPY INTO CAPSTONE_EXAM.BRONZE.banking_raw (
    transaction_id, transaction_date, transaction_type, amount, currency, transaction_status, 
    payment_channel, payment_method, running_balance, is_flagged_fraud, merchant_name, 
    merchant_category, customer_id, customer_name, customer_dob, customer_gender, 
    customer_email, customer_phone, customer_city, customer_country, customer_segment, 
    kyc_status, customer_annual_income, credit_score, account_id, account_type, 
    account_open_date, account_status, branch_id, branch_name, credit_card_id, 
    card_type, card_network, credit_limit, card_status, loan_id, loan_type, 
    loan_amount, interest_rate, tenure_months, loan_status, complaint_id, 
    complaint_category, complaint_priority, complaint_status, source_file_name
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 
        $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, 
        $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, 
        METADATA$FILENAME
    FROM @CAPSTONE_EXAM.PRACTICE.PRAC_STAGE
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
"""

with DAG(
    dag_id="END_TO_END_PIPELINE",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice", "e2e"],
) as dag:
    
    stage_task = upload_files_to_stage()
    
    create_bronze_task = SQLExecuteQueryOperator(
        task_id="create_bronze_schema_and_table",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=CREATE_BRONZE_SQL
    )
    
    load_bronze_task = SQLExecuteQueryOperator(
        task_id="load_to_bronze",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=COPY_INTO_BRONZE_SQL
    )
    
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="cd /usr/local/airflow/dbt/banking_models && dbt run --profiles-dir ..",
    )

    stage_task >> create_bronze_task >> load_bronze_task >> dbt_run_task
