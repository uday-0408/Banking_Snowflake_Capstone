# Banking Analytics Capstone - Master Project Revision & Refresher Guide
**Comprehensive Crash Course: Apache Airflow & dbt (Data Build Tool)**

This document is designed as a deep-dive refresher for intermediate to advanced data engineers. Instead of raw code dumps, it focuses on the **syntax, mechanics, essential concepts, and "gotchas"** of Airflow and dbt, tied directly to the architecture of our Banking ELT Pipeline.

---

## Table of Contents
1. [Part 1: Apache Airflow Refresher](#part-1-apache-airflow-refresher)
    - Core Components & Architecture
    - DAG Syntax & Instantiation
    - Operators vs. TaskFlow API (`@task`)
    - Task Dependencies & Control Flow
    - XComs & Context Variables (Macros)
    - Idempotency, Catchup, and Scheduling
    - Connections & Hooks
2. [Part 2: dbt (data build tool) Refresher](#part-2-dbt-data-build-tool-refresher)
    - Core Concepts & Project Structure
    - Materializations Deep Dive
    - The Magic of Incremental Models
    - Jinja Templating & Macros
    - Testing & Assertions
    - Documentation & `schema.yml`
    - Snapshots & Seeds
3. [Part 3: Orchestrating dbt inside Airflow](#part-3-orchestrating-dbt-inside-airflow)
    - BashOperator vs Astronomer Cosmos
4. [Part 4: Common Troubleshooting & Pitfalls](#part-4-common-troubleshooting--pitfalls)

---

# Part 1: Apache Airflow Refresher

Airflow is not a data processing engine; it is a **task orchestrator**. It tells systems *when* and *in what order* to execute jobs.

### 1. Core Components & Architecture
- **Webserver**: The UI where you monitor DAGs and trigger runs.
- **Scheduler**: The heartbeat of Airflow. It parses DAG files every few seconds and sends ready tasks to the queue.
- **Worker**: The node that actually executes the code.
- **Metadata Database** (usually Postgres): Stores the state of all tasks, DAG runs, variables, and connections.

### 2. DAG Syntax & Instantiation
A DAG (Directed Acyclic Graph) is just a Python file. 

**Standard Context Manager Syntax (Recommended):**
```python
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering_team',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="daily_banking_pipeline",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["finance", "elt"]
) as dag:
    # Tasks go here
```
*Essential Concept*: DAG files are parsed constantly by the scheduler. **Never put heavy computation or database connections at the top level of a DAG file.** All heavy lifting must happen *inside* a task.

### 3. Operators vs. TaskFlow API (`@task`)

**Classic Operators:**
Classes designed to do one specific thing.
- `BashOperator`: Runs terminal commands (e.g., `dbt run`).
- `SQLExecuteQueryOperator`: Runs SQL against a database.
- `S3KeySensor`: Waits for a file to land in an S3 bucket before continuing.

**The TaskFlow API (`@task`):**
Instead of using the clunky classic `PythonOperator`, Airflow 2.0 introduced the `@task` decorator. It allows you to write pure Python functions that Airflow automatically wraps into tasks.
```python
from airflow.decorators import task

@task
def extract_data():
    return {"status": "success", "file": "data.csv"}

@task
def process_data(payload):
    print(f"Processing {payload['file']}")

# Defining dependencies implicitly
data_payload = extract_data()
process_data(data_payload)
```

### 4. Task Dependencies & Control Flow
Airflow uses bitshift operators to define order:
```python
task_1 >> task_2  # task_1 runs first, then task_2
[task_2a, task_2b] << task_1  # task_1 runs, then both 2a and 2b run in parallel
task_1 >> [task_2a, task_2b] >> task_3 # 2a and 2b must BOTH finish before 3 starts
```
*Note*: If `task_2a` fails, `task_3` is skipped by default (unless you change its `trigger_rule`).

### 5. XComs & Context Variables (Macros)
**XComs (Cross-Communication):**
Tasks are isolated (they might even run on different physical machines). They cannot share variables directly. XComs allow tasks to pass small amounts of metadata (like a file path or a row count) via the Airflow Database.
*Warning*: Do NOT pass large dataframes through XComs.

**Macros (Jinja Templating in Airflow):**
Airflow allows you to inject dynamic runtime variables into strings.
```python
# {{ ds }} is injected at runtime with the execution date (YYYY-MM-DD)
run_sql = SQLExecuteQueryOperator(
    task_id="delete_old_data",
    sql="DELETE FROM table WHERE date = '{{ ds }}';"
)
```

### 6. Idempotency, Catchup, and Scheduling
- **Idempotency**: A task should produce the exact same result whether it runs 1 time or 100 times. Never write `INSERT INTO` without checking if the data already exists. Use `COPY INTO` or `MERGE`.
- **Catchup (`catchup=True/False`)**: If you pause a daily DAG for a week and then unpause it, `catchup=True` will immediately spawn 7 DAG runs for the missing days. Always set `catchup=False` unless you explicitly want to backfill data.

### 7. Connections & Hooks
- **Connections**: Securely store passwords and URIs in the Airflow UI (or Astro Secrets). 
- **Hooks**: A Hook is a high-level interface to an external system. For example, `SnowflakeHook` automatically retrieves your connection password, handles connection pooling, and gives you a cursor to execute queries, saving you from writing `sqlalchemy` boilerplate.

---

# Part 2: dbt (data build tool) Refresher

dbt is the "T" in ELT. It assumes your raw data is already sitting in Snowflake. You write `SELECT` statements, and dbt wraps them in DDL (`CREATE TABLE`, `CREATE VIEW`) and executes them in Snowflake.

### 1. Core Concepts & Project Structure
- **Models**: A single `.sql` file containing a `SELECT` statement.
- **Sources**: Defined in `source.yml`. Maps to your raw Bronze tables so you can reference them cleanly via `{{ source('bronze', 'banking_raw') }}`.
- **Target/Profiles**: Configured in `profiles.yml`. Defines the warehouse, role, and schema where models will be built.

### 2. Materializations Deep Dive
You configure how a model is physically built in Snowflake using the `{{ config() }}` block at the top of a file.
1. **View (`materialized='view'`)**: The default. Creates a logical view. Fast to build, but recalculates upon every query in Snowflake. Used for final BI/KPI layers.
2. **Table (`materialized='table'`)**: Drops the old table and completely recreates it. Used for complex joins where query performance is critical, but the dataset is small enough to rebuild daily.
3. **Incremental (`materialized='incremental'`)**: Appends or merges *only new data* into an existing table. Used for massive fact tables (like billions of banking transactions).
4. **Ephemeral (`materialized='ephemeral'`)**: Does not create an object in Snowflake. Instead, dbt injects the SQL as a CTE (Common Table Expression) into any downstream model that references it.

### 3. The Magic of Incremental Models
Writing incremental models is the hardest but most important part of dbt for scaling.
```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

SELECT * FROM {{ ref('banking_clean') }}

{% if is_incremental() %}
  -- This block ONLY injects if the table already exists in Snowflake.
  WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}
```
**How it works:**
- `{{ this }}` refers to the target table being built.
- During the first run, `is_incremental()` is False. dbt builds a `CREATE TABLE` and processes all historical data.
- On the second run, `is_incremental()` is True. dbt builds a temporary table containing *only* records loaded after the max timestamp.
- Because we defined `unique_key='transaction_id'`, dbt will `MERGE` the new data into the old data. If an old transaction was updated, it overwrites it. If it's new, it inserts it.

### 4. Jinja Templating & Macros
Jinja allows you to use control structures (if/for) and variables inside SQL.
- `{{ ref('model_name') }}`: Dynamically replaces itself with the actual schema and table name of the target model, and tells dbt to build the target model *before* this one.
- **Macros**: Reusable SQL functions.
  ```sql
  -- In macros/cents_to_dollars.sql
  {% macro cents_to_dollars(column_name) %}
      ({{ column_name }} / 100)::DECIMAL(16, 2)
  {% endmacro %}
  
  -- In your model:
  SELECT {{ cents_to_dollars('amount_in_cents') }} AS amount
  ```

### 5. Testing & Assertions
dbt comes with built-in generic tests defined in `schema.yml`.
```yaml
models:
  - name: fact_transaction
    columns:
      - name: transaction_id
        tests:
          - unique
          - not_null
      - name: transaction_status
        tests:
          - accepted_values:
              values: ['SUCCESS', 'FAILED', 'PENDING']
```
When you run `dbt test`, dbt writes SQL to check these rules. If a `transaction_id` is null, the test fails, and your Airflow pipeline alerts you.

### 6. Snapshots (Slowly Changing Dimensions)
If your raw database updates a customer's address, the old address is lost. dbt Snapshots track historical changes automatically (Type 2 SCD).
- You define a snapshot block.
- dbt adds `dbt_valid_from` and `dbt_valid_to` columns.
- When you run `dbt snapshot`, it compares the raw table to the snapshot table, and automatically expires old records and inserts new ones if a watched column changed.

---

# Part 3: Orchestrating dbt inside Airflow

### The `BashOperator` approach
The simplest way to run dbt is to trigger the command via bash.
```python
dbt_run = BashOperator(
    task_id="run_dbt_models",
    bash_command="dbt run --profiles-dir /usr/local/airflow/dbt"
)
```
- **Syntax to remember**: You often need `--profiles-dir` to point to the mounted credentials folder.
- **Flaw**: If `dbt run` fails on model 49 out of 50, the *entire* Airflow task fails. You have to restart from model 1.

### The Astronomer Cosmos approach
Cosmos parses your `manifest.json` and renders your dbt DAG natively in Airflow.
```python
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from pathlib import Path

dbt_tg = DbtTaskGroup(
    group_id="dbt_transformations",
    project_config=ProjectConfig("/usr/local/airflow/dbt"),
    profile_config=ProfileConfig(
        profile_name="banking_models",
        target_name="dev",
        profiles_yml_filepath=Path("/usr/local/airflow/dbt/profiles.yml")
    )
)
```
- **Advantage**: Every dbt model is a green/red box in Airflow. If a model fails, you clear *only* that model in Airflow, and it restarts from there.

---

# Part 4: Common Troubleshooting & Pitfalls

### 1. Airflow: "File not found" when executing dbt
- **The Issue**: Airflow uses Docker. If your host path is `C:\Code\dbt` but you mapped it to `/opt/airflow/dbt` in `docker-compose`, Airflow *cannot* see `C:\Code\dbt`. 
- **The Rule**: Always use the container's mapped path in Bash commands.

### 2. dbt: Static Parser CRLF Bug (`unable to infer all dependencies`)
- **The Issue**: dbt parses `{{ ref('model') }}` to build the DAG. On Windows, lines end with `\r\n`. Inside the Linux Airflow container, dbt hits the `\r` and the parser crashes, asking you to add `-- depends_on: {{ ref('model') }}`.
- **The Rule**: Never save SQL files in `CRLF` mode if you are deploying to Linux. Always configure your IDE to save as `LF` (Unix).

### 3. Airflow: Catchup filling up Database
- **The Issue**: A developer sets `start_date=datetime(2020, 1, 1)` and `catchup=True`. Airflow immediately attempts to run the DAG 1000+ times to catch up to today, crashing the database.
- **The Rule**: Always explicitly set `catchup=False` during development.

### 4. Snowflake: Unintended Full Table Scans in dbt
- **The Issue**: An incremental model is written incorrectly without the `{% if is_incremental() %}` block. Every time it runs, it scans 1 billion rows.
- **The Rule**: Always wrap your delta-filter in `is_incremental()`. Verify it works by checking the Snowflake Query History. The resulting query should have a `WHERE loaded_at > ...` clause.


---

# Part 5: Appendix: Complete Source Code Reference

The following section contains the full source code for every component of the pipeline, serving as a comprehensive reference.

### A. Airflow DAGs & Operators

#### end_to_end_pipeline.py
`python
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

`

#### load_to_stage.py
`python
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


DATA_DIR = Path("/usr/local/airflow/data/raw")
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_STAGE = "CAPSTONE_EXAM.PRACTICE.PRAC_STAGE"

with DAG(
    dag_id="LOAD_TO_STAGE",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["practice"],
) as dag:

    @task
    def upload_files_to_stage():
        # 1. Find CSV files inside the mounted directory.
        csv_files = sorted(DATA_DIR.glob("*.csv"))

        if not csv_files:
            raise AirflowException(
                f"No CSV files found in {DATA_DIR}"
            )

        # 2. Open the Snowflake connection.
        hook = SnowflakeHook(
            snowflake_conn_id=SNOWFLAKE_CONN_ID
        )

        connection = hook.get_conn()
        cursor = connection.cursor()

        try:
            # 3. Upload every discovered CSV file.
            for file_path in csv_files:
                file_uri = f"file://{file_path.resolve().as_posix()}"
                print("File url: ",file_uri) 

                # Fixed the missing 'f' prefix and spacing below
                put_sql = (
                    f"PUT '{file_uri}' "
                    f"@{SNOWFLAKE_STAGE} "
                    "AUTO_COMPRESS=TRUE "
                    "OVERWRITE=FALSE"
                )

                cursor.execute(put_sql)

                result = cursor.fetchall()

                print(
                    f"Upload result for {file_path.name}: {result}"
                )

        finally:
            cursor.close()
            connection.close()

    # Invoke the task
    upload_files_to_stage()
`

#### stage_operations.py
`python
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

`

### B. dbt Configurations & Profiles

#### banking_dbt\profiles.yml
`yaml
IU_Account:
  outputs:
    dev:
      account: ZBGJUPU-BY01332
      user: IU_DBT_SERVICE_2
      password: Pass@1234567890
      role: DBT_MACHINE
      warehouse: COMPUTE_WH
      database: CAPSTONE_EXAM
      schema: PRACTICE
      threads: 1
      type: snowflake
  target: dev
banking_models:
  outputs:
    dev:
      account: ZBGJUPU-BY01332
      user: IU_DBT_SERVICE_2
      password: Pass@1234567890
      role: DBT_MACHINE
      warehouse: COMPUTE_WH
      database: CAPSTONE_EXAM
      schema: PRACTICE
      threads: 1
      type: snowflake
  target: dev
`

#### banking_dbt\banking_models\models\source.yml
`yaml
version: 2

sources:
  - name: bronze
    schema: BRONZE

    tables:
      - name: banking_raw

`

#### banking_dbt\banking_models\models\gold\dims\dims.yml
`yaml
version: 2

models:
  - name: dim_branch
    description: 'Gold layer model: dim_branch'
  - name: dim_customer
    description: 'Gold layer model: dim_customer'
  - name: dim_date
    description: 'Gold layer model: dim_date'

`

#### banking_dbt\banking_models\models\gold\facts\facts.yml
`yaml
version: 2

models:
  - name: fact_complaint
    description: 'Gold layer model: fact_complaint'
  - name: fact_loan
    description: 'Gold layer model: fact_loan'
  - name: fact_transaction
    description: 'Gold layer model: fact_transaction'

`

#### banking_dbt\banking_models\models\gold\kpis\kpis.yml
`yaml
version: 2

models:
  - name: kpi_01_avg_transaction_amount
    description: 'Gold layer model: kpi_01_avg_transaction_amount'
  - name: kpi_02_payment_method_usage
    description: 'Gold layer model: kpi_02_payment_method_usage'
  - name: kpi_03_transaction_success_rate
    description: 'Gold layer model: kpi_03_transaction_success_rate'
  - name: kpi_04_transaction_value_by_segment
    description: 'Gold layer model: kpi_04_transaction_value_by_segment'
  - name: kpi_05_mom_transaction_value
    description: 'Gold layer model: kpi_05_mom_transaction_value'
  - name: kpi_06_estimated_interest_income
    description: 'Gold layer model: kpi_06_estimated_interest_income'
  - name: kpi_07_subprime_default_rate
    description: 'Gold layer model: kpi_07_subprime_default_rate'
  - name: kpi_08_complaint_resolution_rate
    description: 'Gold layer model: kpi_08_complaint_resolution_rate'
  - name: kpi_09_complaint_priority_distribution
    description: 'Gold layer model: kpi_09_complaint_priority_distribution'
  - name: kpi_10_top_5_customers_net_exposure
    description: 'Gold layer model: kpi_10_top_5_customers_net_exposure'

`

#### banking_dbt\banking_models\models\gold\monthly_views\monthly_views.yml
`yaml
version: 2

models:
  - name: monthly_branch_performance_view
    description: 'Gold layer model: monthly_branch_performance_view'
  - name: monthly_customer_performance_view
    description: 'Gold layer model: monthly_customer_performance_view'
  - name: monthly_transaction_view
    description: 'Gold layer model: monthly_transaction_view'

`

#### banking_dbt\banking_models\models\silver\silver.yml
`yaml
version: 2

models:
  - name: banking_clean

    description: >
      Cleaned banking transaction data. The model converts raw string
      values into appropriate Snowflake data types, standardizes text,
      cleans malformed emails and phone numbers, and preserves source
      ingestion metadata.

    columns:
      - name: transaction_id
        description: "Unique identifier for the banking transaction."
        tests:
          - not_null
          - unique

      - name: transaction_date
        description: "Date and time when the transaction occurred."
        tests:
          - not_null

      - name: transaction_type
        description: "Type of banking transaction."
        tests:
          - not_null

      - name: amount
        description: "Transaction amount."
        tests:
          - not_null

      - name: currency
        description: "Currency of the transaction."
        tests:
          - not_null

      - name: transaction_status
        description: "Current status of the transaction."
        tests:
          - not_null
          - accepted_values:
              arguments:
                values:
                  - SUCCESS
                  - FAILED
                  - REVERSED
                  - PENDING
                  - DECLINED

      - name: payment_channel
        description: "Channel through which the transaction was initiated."

      - name: payment_method
        description: "Payment method used for the transaction."

      - name: running_balance
        description: "Account balance associated with the transaction."

      - name: is_flagged_fraud
        description: "Indicates whether the transaction is flagged as fraudulent."

      - name: customer_id
        description: "Business identifier of the customer."
        tests:
          - not_null

      - name: customer_name
        description: "Customer's name."

      - name: customer_dob
        description: "Customer date of birth."

      - name: customer_gender
        description: "Customer gender."

      - name: customer_email
        description: "Cleaned customer email address."

      - name: customer_phone
        description: "Cleaned customer phone number."

      - name: customer_city
        description: "Customer city."

      - name: customer_country
        description: "Customer country."

      - name: customer_segment
        description: "Customer segment."

      - name: kyc_status
        description: "Customer KYC status."

      - name: customer_annual_income
        description: "Customer annual income."

      - name: credit_score
        description: "Customer credit score."

      - name: account_id
        description: "Business identifier of the account."
        tests:
          - not_null

      - name: account_type
        description: "Type of bank account."

      - name: account_open_date
        description: "Date when the account was opened."

      - name: account_status
        description: "Current account status."

      - name: branch_id
        description: "Business identifier of the branch."
        tests:
          - not_null

      - name: branch_name
        description: "Name of the bank branch."

      - name: credit_card_id
        description: "Business identifier of the credit card."

      - name: card_type
        description: "Credit card type."

      - name: card_network
        description: "Credit card network."

      - name: credit_limit
        description: "Credit card limit."

      - name: card_status
        description: "Credit card status."

      - name: loan_id
        description: "Business identifier of the loan."

      - name: loan_type
        description: "Type of loan."

      - name: loan_amount
        description: "Loan principal amount."

      - name: interest_rate
        description: "Loan interest rate."

      - name: tenure_months
        description: "Loan tenure in months."

      - name: loan_status
        description: "Current loan status."

      - name: complaint_id
        description: "Business identifier of the complaint."

      - name: complaint_category
        description: "Category of customer complaint."

      - name: complaint_priority
        description: "Priority of customer complaint."

      - name: complaint_status
        description: "Current complaint status."

      - name: source_file_name
        description: "Source file from which the record was loaded."

      - name: loaded_at
        description: "Timestamp when the record was loaded into Bronze."

`

### C. dbt Transformation Models (Silver & Gold)

#### banking_dbt\banking_models\models\gold\dims\dim_branch.sql
`sql
-- depends_on: {{ ref('banking_clean') }}
{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    branch_id,
    branch_name
FROM {{ ref('banking_clean') }}
WHERE branch_id IS NOT NULL

`

#### banking_dbt\banking_models\models\gold\dims\dim_customer.sql
`sql
-- depends_on: {{ ref('banking_clean') }}
{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    customer_id,
    customer_name,
    customer_segment,
    credit_score
FROM {{ ref('banking_clean') }}
WHERE customer_id IS NOT NULL

`

#### banking_dbt\banking_models\models\gold\dims\dim_date.sql
`sql
-- depends_on: {{ ref('banking_clean') }}
{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    TO_DATE(transaction_date) AS date_key,
    EXTRACT(YEAR FROM transaction_date) AS year,
    EXTRACT(MONTH FROM transaction_date) AS month,
    EXTRACT(QUARTER FROM transaction_date) AS quarter
FROM {{ ref('banking_clean') }}
WHERE transaction_date IS NOT NULL

`

#### banking_dbt\banking_models\models\gold\facts\fact_complaint.sql
`sql
-- depends_on: {{ ref('banking_clean') }}
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='complaint_id'
) }}

SELECT DISTINCT
    complaint_id,
    customer_id,
    branch_id,
    TO_DATE(transaction_date) AS date_key,
    complaint_category,
    complaint_priority,
    complaint_status,
    loaded_at
FROM {{ ref('banking_clean') }}
WHERE complaint_id IS NOT NULL

{% if is_incremental() %}
  AND loaded_at > (SELECT COALESCE(MAX(t.loaded_at), '1900-01-01') FROM {{ this }} t)
{% endif %}

`

#### banking_dbt\banking_models\models\gold\facts\fact_loan.sql
`sql
-- depends_on: {{ ref('banking_clean') }}
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='loan_id'
) }}

SELECT DISTINCT
    loan_id,
    customer_id,
    branch_id,
    TO_DATE(transaction_date) AS date_key,
    loan_type,
    loan_amount,
    interest_rate,
    tenure_months,
    loan_status,
    loaded_at
FROM {{ ref('banking_clean') }}
WHERE loan_id IS NOT NULL

{% if is_incremental() %}
  AND loaded_at > (SELECT COALESCE(MAX(t.loaded_at), '1900-01-01') FROM {{ this }} t)
{% endif %}

`

#### banking_dbt\banking_models\models\gold\facts\fact_transaction.sql
`sql
-- depends_on: {{ ref('banking_clean') }}
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='transaction_id'
) }}

SELECT
    transaction_id,
    customer_id,
    branch_id,
    loan_id,
    transaction_date AS date_key,
    transaction_type,
    amount,
    payment_channel,
    payment_method,
    transaction_status,
    is_flagged_fraud,
    loaded_at
FROM {{ ref('banking_clean') }}
WHERE transaction_id IS NOT NULL

{% if is_incremental() %}
  -- Only process new data loaded since the last run
  AND loaded_at > (SELECT COALESCE(MAX(t.loaded_at), '1900-01-01') FROM {{ this }} t)
{% endif %}

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_01_avg_transaction_amount.sql
`sql
-- depends_on: {{ ref('fact_transaction') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    transaction_type,
    AVG(amount) as average_transaction_amount
FROM {{ ref('fact_transaction') }}
GROUP BY transaction_type

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_02_payment_method_usage.sql
`sql
-- depends_on: {{ ref('fact_transaction') }}
{{ config(materialized='view', schema='gold') }}

SELECT
    payment_method,
    COUNT(transaction_id) as transaction_count
FROM {{ ref('fact_transaction') }}
GROUP BY payment_method
ORDER BY transaction_count DESC

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_03_transaction_success_rate.sql
`sql
-- depends_on: {{ ref('fact_transaction') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    payment_channel,
    COUNT(transaction_id) AS total_transactions,
    SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
    ROUND((SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(transaction_id)) * 100, 2) AS success_rate
FROM {{ ref('fact_transaction') }}
GROUP BY payment_channel

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_04_transaction_value_by_segment.sql
`sql
-- depends_on: {{ ref('fact_transaction') }}
-- depends_on: {{ ref('dim_customer') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    dc.customer_segment,
    COUNT(ft.transaction_id) AS transaction_count,
    SUM(ft.amount) AS total_transaction_value,
    AVG(ft.amount) AS average_transaction_value
FROM {{ ref('fact_transaction') }} ft
JOIN {{ ref('dim_customer') }} dc ON ft.customer_id = dc.customer_id
GROUP BY dc.customer_segment

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_05_mom_transaction_value.sql
`sql
-- depends_on: {{ ref('fact_transaction') }}
-- depends_on: {{ ref('dim_date') }}
{{ config(materialized='view', schema='gold') }}

WITH monthly_value AS (
    SELECT 
        dd.month AS transaction_month,
        dd.year AS transaction_year,
        SUM(ft.amount) AS total_transaction_value
    FROM {{ ref('fact_transaction') }} ft
    JOIN {{ ref('dim_date') }} dd ON ft.date_key = dd.date_key
    GROUP BY dd.year, dd.month
)
SELECT 
    transaction_month,
    total_transaction_value,
    LAG(total_transaction_value) OVER (ORDER BY transaction_year, transaction_month) AS previous_month_value,
    ROUND((total_transaction_value - LAG(total_transaction_value) OVER (ORDER BY transaction_year, transaction_month)) / 
          LAG(total_transaction_value) OVER (ORDER BY transaction_year, transaction_month) * 100, 2) AS mom_growth_percentage
FROM monthly_value

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_06_estimated_interest_income.sql
`sql
-- depends_on: {{ ref('fact_loan') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    loan_type,
    COUNT(loan_id) AS loan_count,
    SUM(loan_amount) AS total_loan_exposure,
    AVG(interest_rate) AS average_interest_rate,
    AVG(tenure_months) AS average_tenure_months,
    SUM(loan_amount * interest_rate * tenure_months / 12) AS estimated_interest
FROM {{ ref('fact_loan') }}
GROUP BY loan_type

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_07_subprime_default_rate.sql
`sql
-- depends_on: {{ ref('dim_date') }}
-- depends_on: {{ ref('fact_loan') }}
-- depends_on: {{ ref('dim_customer') }}
-- depends_on: {{ ref('dim_branch') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    db.branch_name,
    fl.loan_type,
    COUNT(fl.loan_id) AS total_subprime_loans,
    SUM(CASE WHEN fl.loan_status = 'DEFAULT' THEN 1 ELSE 0 END) AS defaulted_loans,
    ROUND((SUM(CASE WHEN fl.loan_status = 'DEFAULT' THEN 1 ELSE 0 END) / COUNT(fl.loan_id)) * 100, 2) AS default_rate
FROM {{ ref('fact_loan') }} fl
JOIN {{ ref('dim_customer') }} dc ON fl.customer_id = dc.customer_id
JOIN {{ ref('dim_branch') }} db ON fl.branch_id = db.branch_id
JOIN {{ ref('dim_date') }} dd ON fl.date_key = dd.date_key
WHERE dc.credit_score < 650 
  AND dd.year = 2026 
  AND dd.quarter = 2
GROUP BY db.branch_name, fl.loan_type

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_08_complaint_resolution_rate.sql
`sql
-- depends_on: {{ ref('dim_branch') }}
-- depends_on: {{ ref('fact_complaint') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    db.branch_name,
    fc.complaint_category,
    COUNT(fc.complaint_id) AS total_complaints,
    SUM(CASE WHEN fc.complaint_status = 'RESOLVED' THEN 1 ELSE 0 END) AS resolved_complaints,
    ROUND((SUM(CASE WHEN fc.complaint_status = 'RESOLVED' THEN 1 ELSE 0 END) / COUNT(fc.complaint_id)) * 100, 2) AS resolution_rate
FROM {{ ref('fact_complaint') }} fc
JOIN {{ ref('dim_branch') }} db ON fc.branch_id = db.branch_id
GROUP BY db.branch_name, fc.complaint_category

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_09_complaint_priority_distribution.sql
`sql
-- depends_on: {{ ref('dim_branch') }}
-- depends_on: {{ ref('fact_complaint') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    db.branch_name,
    SUM(CASE WHEN fc.complaint_priority = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_priority_complaints,
    SUM(CASE WHEN fc.complaint_priority = 'HIGH' THEN 1 ELSE 0 END) AS high_priority_complaints,
    SUM(CASE WHEN fc.complaint_priority = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_priority_complaints,
    SUM(CASE WHEN fc.complaint_priority = 'LOW' THEN 1 ELSE 0 END) AS low_priority_complaints
FROM {{ ref('fact_complaint') }} fc
JOIN {{ ref('dim_branch') }} db ON fc.branch_id = db.branch_id
GROUP BY db.branch_name

`

#### banking_dbt\banking_models\models\gold\kpis\kpi_10_top_5_customers_net_exposure.sql
`sql
-- depends_on: {{ ref('fact_transaction') }}
-- depends_on: {{ ref('fact_loan') }}
-- depends_on: {{ ref('dim_customer') }}
-- depends_on: {{ ref('dim_branch') }}
{{ config(materialized='view', schema='gold') }}

WITH loan_repayments AS (
    SELECT 
        loan_id, 
        SUM(amount) AS total_repayments
    FROM {{ ref('fact_transaction') }} 
    WHERE transaction_type = 'REPAYMENT'
    GROUP BY loan_id
),
customer_exposure AS (
    SELECT 
        db.branch_name,
        dc.customer_id,
        dc.customer_name,
        fl.loan_type,
        fl.loan_amount,
        COALESCE(lr.total_repayments, 0) AS total_repayments,
        (fl.loan_amount - COALESCE(lr.total_repayments, 0)) AS net_loan_exposure
    FROM {{ ref('fact_loan') }} fl
    JOIN {{ ref('dim_customer') }} dc ON fl.customer_id = dc.customer_id
    JOIN {{ ref('dim_branch') }} db ON fl.branch_id = db.branch_id
    LEFT JOIN loan_repayments lr ON fl.loan_id = lr.loan_id
    WHERE fl.loan_status IN ('OPEN', 'RESTRUCTURED')
),
ranked_exposure AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY branch_name ORDER BY net_loan_exposure DESC) AS rank_in_branch
    FROM customer_exposure
)
SELECT * FROM ranked_exposure WHERE rank_in_branch <= 5

`

#### banking_dbt\banking_models\models\gold\monthly_views\monthly_branch_performance_view.sql
`sql
-- depends_on: {{ ref('fact_transaction') }}
-- depends_on: {{ ref('dim_date') }}
-- depends_on: {{ ref('dim_branch') }}
{{ config(materialized='view', schema='gold') }}

WITH branch_monthly AS (
    SELECT 
        db.branch_id,
        db.branch_name,
        dd.month AS transaction_month,
        dd.year AS transaction_year,
        COUNT(ft.transaction_id) AS transaction_count,
        SUM(ft.amount) AS total_transaction_amount
    FROM {{ ref('fact_transaction') }} ft
    JOIN {{ ref('dim_branch') }} db ON ft.branch_id = db.branch_id
    JOIN {{ ref('dim_date') }} dd ON ft.date_key = dd.date_key
    GROUP BY db.branch_id, db.branch_name, dd.year, dd.month
)
SELECT 
    branch_id,
    branch_name,
    transaction_month,
    transaction_year,
    transaction_count,
    total_transaction_amount,
    LAG(total_transaction_amount) OVER (PARTITION BY branch_id ORDER BY transaction_year, transaction_month) AS previous_month_amount,
    ROUND((total_transaction_amount - LAG(total_transaction_amount) OVER (PARTITION BY branch_id ORDER BY transaction_year, transaction_month)) / 
          NULLIF(LAG(total_transaction_amount) OVER (PARTITION BY branch_id ORDER BY transaction_year, transaction_month), 0) * 100, 2) AS mom_growth_pct,
    RANK() OVER (PARTITION BY transaction_year, transaction_month ORDER BY total_transaction_amount DESC) AS branch_rank
FROM branch_monthly

`

#### banking_dbt\banking_models\models\gold\monthly_views\monthly_customer_performance_view.sql
`sql
-- depends_on: {{ ref('fact_transaction') }}
-- depends_on: {{ ref('dim_customer') }}
-- depends_on: {{ ref('dim_date') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    ft.customer_id,
    dd.month as transaction_month,
    dd.year as transaction_year,
    dc.customer_name,
    dc.customer_segment,
    SUM(amount) as current_month_transaction_amount,
    LAG(SUM(amount)) OVER(PARTITION BY ft.customer_id ORDER BY dd.year, dd.month) as previous_month_transaction_amount,
    LEAD(SUM(amount)) OVER(PARTITION BY ft.customer_id ORDER BY dd.year, dd.month) as next_month_transaction_amount,
    ROUND(((SUM(amount) - LAG(SUM(amount)) OVER(PARTITION BY ft.customer_id ORDER BY dd.year, dd.month)) / NULLIF(LAG(SUM(amount)) OVER(PARTITION BY ft.customer_id ORDER BY dd.year, dd.month), 0)) * 100, 2) as mom_growth_pct,
    RANK() OVER(PARTITION BY dc.customer_segment, dd.year, dd.month ORDER BY SUM(amount) DESC) as customer_rank
FROM {{ ref('fact_transaction') }} ft
JOIN {{ ref('dim_customer') }} dc ON ft.customer_id = dc.customer_id
JOIN {{ ref('dim_date') }} dd ON ft.date_key = dd.date_key
GROUP BY ft.customer_id, dd.month, dd.year, dc.customer_name, dc.customer_segment

`

#### banking_dbt\banking_models\models\gold\monthly_views\monthly_transaction_view.sql
`sql
-- depends_on: {{ ref('fact_transaction') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    transaction_type,
    COUNT(transaction_id) as total_transactions,
    SUM(amount) as total_transaction_amount,
    AVG(amount) as average_amount,
    SUM(CASE WHEN transaction_status='SUCCESS' THEN 1 ELSE 0 END) as successful_transaction_count,
    SUM(CASE WHEN transaction_status='FAILED' THEN 1 ELSE 0 END) as failed_transaction_count,
    SUM(CASE WHEN is_flagged_fraud=True THEN 1 ELSE 0 END) as fraud_transaction_count,
    SUM(CASE WHEN is_flagged_fraud=True THEN amount ELSE 0 END) as fraud_amount,
    ROUND((SUM(CASE WHEN transaction_status='SUCCESS' THEN 1 ELSE 0 END) / COUNT(transaction_id)) * 100, 2) as success_rate
FROM {{ ref('fact_transaction') }}
GROUP BY transaction_type

`

#### banking_dbt\banking_models\models\silver\banking_clean.sql
`sql
{{ config(
    materialized='incremental',
    schema='silver'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'banking_raw') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT COALESCE(MAX(loaded_at), '1900-01-01') FROM {{ this }})
    {% endif %}

),

cleaned AS (

    SELECT

        /* =========================================================
           TRANSACTION
           ========================================================= */

        NULLIF(TRIM(transaction_id), '') AS transaction_id,

        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'MM/DD/YYYY'),
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'DD-MM-YYYY'),
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'YYYY-MM-DD'),
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'DD/MM/YYYY')
        ) AS transaction_date,

        UPPER(NULLIF(TRIM(transaction_type), ''))
            AS transaction_type,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(amount), ''),
            18,
            2
        ) AS amount,

        UPPER(NULLIF(TRIM(currency), ''))
            AS currency,

        UPPER(NULLIF(TRIM(transaction_status), ''))
            AS transaction_status,

        UPPER(NULLIF(TRIM(payment_channel), ''))
            AS payment_channel,

        UPPER(NULLIF(TRIM(payment_method), ''))
            AS payment_method,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(running_balance), ''),
            18,
            2
        ) AS running_balance,

        TRY_TO_BOOLEAN(
            NULLIF(TRIM(is_flagged_fraud), '')
        ) AS is_flagged_fraud,


        /* =========================================================
           MERCHANT
           ========================================================= */

        NULLIF(TRIM(merchant_name), '')
            AS merchant_name,

        UPPER(NULLIF(TRIM(merchant_category), ''))
            AS merchant_category,


        /* =========================================================
           CUSTOMER
           ========================================================= */

        NULLIF(TRIM(customer_id), '')
            AS customer_id,

        NULLIF(TRIM(customer_name), '')
            AS customer_name,

        COALESCE(
            TRY_TO_DATE(TRIM(customer_dob), 'YYYY-MM-DD'),
            TRY_TO_DATE(TRIM(customer_dob), 'MM/DD/YYYY'),
            TRY_TO_DATE(TRIM(customer_dob), 'DD-MM-YYYY'),
            TRY_TO_DATE(TRIM(customer_dob), 'DD/MM/YYYY')
        ) AS customer_dob,

        CASE
            WHEN UPPER(TRIM(customer_gender)) IN ('M', 'MALE')
                THEN 'M'

            WHEN UPPER(TRIM(customer_gender)) IN ('F', 'FEMALE')
                THEN 'F'

            WHEN UPPER(TRIM(customer_gender)) = 'OTHER'
                THEN 'OTHER'

            ELSE NULL
        END AS customer_gender,

        CASE

            WHEN LOWER(TRIM(customer_email))
                 LIKE '%_at_%'
            THEN REPLACE(
                LOWER(TRIM(customer_email)),
                '_at_',
                '@'
            )

            WHEN LOWER(TRIM(customer_email))
                 LIKE '%@%.%'
            THEN LOWER(TRIM(customer_email))

            ELSE NULL

        END AS customer_email,

        CASE

            WHEN REGEXP_LIKE(
                TRIM(customer_phone),
                '^[0-9]+\.0$'
            )
            THEN REGEXP_REPLACE(
                TRIM(customer_phone),
                '\.0$',
                ''
            )

            WHEN REGEXP_LIKE(
                TRIM(customer_phone),
                '^[0-9]+$'
            )
            THEN TRIM(customer_phone)

            ELSE NULL

        END AS customer_phone,

        NULLIF(TRIM(customer_city), '')
            AS customer_city,

        INITCAP(NULLIF(TRIM(customer_country), ''))
            AS customer_country,

        NULLIF(TRIM(customer_segment), '')
            AS customer_segment,

        NULLIF(TRIM(kyc_status), '')
            AS kyc_status,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(customer_annual_income), ''),
            18,
            2
        ) AS customer_annual_income,

        TRY_TO_NUMBER(
            NULLIF(TRIM(credit_score), '')
        ) AS credit_score,


        /* =========================================================
           ACCOUNT
           ========================================================= */

        NULLIF(TRIM(account_id), '')
            AS account_id,

        NULLIF(TRIM(account_type), '')
            AS account_type,

        COALESCE(
            TRY_TO_DATE(TRIM(account_open_date), 'YYYY-MM-DD'),
            TRY_TO_DATE(TRIM(account_open_date), 'MM/DD/YYYY'),
            TRY_TO_DATE(TRIM(account_open_date), 'DD-MM-YYYY'),
            TRY_TO_DATE(TRIM(account_open_date), 'DD/MM/YYYY')
        ) AS account_open_date,

        NULLIF(TRIM(account_status), '')
            AS account_status,


        /* =========================================================
           BRANCH
           ========================================================= */

        NULLIF(TRIM(branch_id), '')
            AS branch_id,

        NULLIF(TRIM(branch_name), '')
            AS branch_name,


        /* =========================================================
           CREDIT CARD
           ========================================================= */

        NULLIF(TRIM(credit_card_id), '')
            AS credit_card_id,

        NULLIF(TRIM(card_type), '')
            AS card_type,

        UPPER(NULLIF(TRIM(card_network), ''))
            AS card_network,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(credit_limit), ''),
            18,
            2
        ) AS credit_limit,

        NULLIF(TRIM(card_status), '')
            AS card_status,


        /* =========================================================
           LOAN
           ========================================================= */

        NULLIF(TRIM(loan_id), '')
            AS loan_id,

        NULLIF(TRIM(loan_type), '')
            AS loan_type,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(loan_amount), ''),
            18,
            2
        ) AS loan_amount,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(interest_rate), ''),
            8,
            4
        ) AS interest_rate,

        TRY_TO_NUMBER(
            NULLIF(TRIM(tenure_months), '')
        ) AS tenure_months,

        NULLIF(TRIM(loan_status), '')
            AS loan_status,


        /* =========================================================
           COMPLAINT
           ========================================================= */

        NULLIF(TRIM(complaint_id), '')
            AS complaint_id,

        NULLIF(TRIM(complaint_category), '')
            AS complaint_category,

        NULLIF(TRIM(complaint_priority), '')
            AS complaint_priority,

        NULLIF(TRIM(complaint_status), '')
            AS complaint_status,


        /* =========================================================
           INGESTION METADATA
           ========================================================= */

        source_file_name,

        loaded_at

    FROM source_data

)

SELECT *
FROM cleaned
`
