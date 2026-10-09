# Banking Analytics Capstone - Master Project Revision & Roadmap

This document serves as a complete, highly detailed step-by-step roadmap to rebuild the Banking Analytics Data Pipeline from scratch. It explains every core concept, from Docker mounting and Astro configurations to Cosmos and Python DAG logic, so any user can understand the "how" and "why" behind the code.

---

## 1. Architecture Overview

The pipeline implements an **ELT** (Extract, Load, Transform) architecture:
1. **Extract**: Raw CSV files (transactions, loans, complaints) exist in a local data directory.
2. **Load (Airflow -> Snowflake Stage -> Bronze)**: Airflow uses a Python Hook to `PUT` files into a Snowflake internal stage, then executes a `COPY INTO` query to load the data into a raw schema (`BRONZE`).
3. **Transform (dbt)**: 
   - **Silver Layer**: Cleans the data, formats timestamps, and casts types.
   - **Gold Layer**: Shapes data into Dimensions, Facts, and KPI views.

---

## 2. Step-by-Step Implementation Roadmap

### Step 1: Environment Setup
To start from scratch, you need to initialize two parallel projects:
- **Airflow Setup**: Run `astro dev init` inside a folder (e.g., `banking_airflow`). This generates a Dockerized Airflow environment.
- **dbt Setup**: Run `dbt init banking_models` in a parallel folder (e.g., `banking_dbt`). 

### Step 2: How to Mount Folders into the Astro Container
Because Airflow runs inside an isolated Docker container, it cannot natively see the `banking_dbt` folder or the `Data` folder sitting on your Windows host machine. You must mount them.

1. **Create `docker-compose.override.yml`** in your Astro directory.
2. **Define Volumes**:
   ```yaml
   services:
     scheduler:
       volumes:
         # Maps the host dbt folder to the container's dbt folder
         - D:/Codes/.../banking_dbt:/usr/local/airflow/dbt:rw
         # Maps the host Data folder to the container's data folder (Read-Only)
         - D:/Codes/.../Data:/usr/local/airflow/data/raw:ro
   ```
   *(Repeat this block for the `dag-processor` and `api-server` services if needed).*

**How to check if the mount was successful:**
If you want to verify that Airflow can see your files, open a terminal in your Astro folder and run:
`astro dev bash` (This opens a shell inside the Airflow container).
Then type: `ls -la /usr/local/airflow/dbt`. If you see your dbt files, the mount worked!

### Step 3: Connecting dbt to Snowflake & Choosing Profiles
dbt uses a `profiles.yml` file to securely connect to your database. 

**How to choose a particular profile:**
Inside `profiles.yml`, you define targets under a profile name:
```yaml
banking_models:
  outputs:
    dev:
      account: <your_account>
      user: <your_user>
      schema: PRACTICE
      ...
    prod:
      schema: PROD_PRACTICE
      ...
  target: dev
```
By default, dbt uses the schema under `target: dev`. If you want to run the production profile in Airflow, you simply pass the flag: `dbt run --target prod`.

**Connecting it in Airflow:**
To ensure Airflow's dbt command finds this file, you map the `profile.yml` into the container via docker overrides, or you simply point to it in the bash command using `--profiles-dir`. (e.g., `dbt run --profiles-dir /usr/local/airflow/dbt`).

### Step 4: Running dbt in Airflow (BashOperator vs. Cosmos)

**Method 1: Using BashOperator (The Simple Way)**
If you don't use Cosmos, you simply tell Airflow to open a terminal inside the container and type out the dbt command, just like a human would:
```python
dbt_run_task = BashOperator(
    task_id="dbt_run",
    bash_command="cd /usr/local/airflow/dbt/banking_models && dbt run --profiles-dir ..",
)
```
- *Pros*: Extremely simple to set up.
- *Cons*: Airflow sees the entire dbt run as ONE giant task. If one single dbt model fails, the entire task fails, and you have to restart the whole dbt run from scratch.

**Method 2: Using Astronomer Cosmos (The Advanced Way)**
Cosmos is an open-source tool that reads your dbt project and automatically translates **every single dbt model into its own individual Airflow task**.
- *How it works*: You import `DbtTaskGroup` from `cosmos`, point it to your `/usr/local/airflow/dbt` project folder, and provide the connection (`snowflake_default`).
- *How to use it*: 
  ```python
  from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
  
  dbt_tg = DbtTaskGroup(
      group_id="dbt_transformations",
      project_config=ProjectConfig("/usr/local/airflow/dbt/banking_models"),
      profile_config=ProfileConfig(profile_name="banking_models", target_name="dev", ...)
  )
  ```
- *Pros*: If a silver model passes but a gold model fails, you can visually see exactly which model failed in the Airflow UI, and click "Clear" to restart *only* that failed model.

---

## 3. High-Level Explanation of the Airflow Scripts

To understand how the pipeline is orchestrated, let's look at the three main Python files:

### A. `load_to_stage.py` (The Concept Tester)
- **Purpose**: This was a basic testing DAG. Before building a massive pipeline, we always build a small DAG to prove that Airflow can successfully talk to Snowflake.
- **How it works**: It uses `SnowflakeHook` to open a connection, finds a single CSV file, and runs a simple `PUT` command. It is meant to be run once for validation.

### B. `stage_operations.py` (The Reusable Logic)
- **Purpose**: This file doesn't run on its own; it contains a reusable Python function (`upload_files_to_stage()`) decorated with `@task` (Airflow's TaskFlow API).
- **How it works**: 
  1. It loops recursively through the mounted `/usr/local/airflow/data/raw` folder to find every single `.csv`.
  2. It connects to Snowflake using `SnowflakeHook(snowflake_conn_id="snowflake_default")`.
  3. It executes a `PUT 'file://<path>' @<stage_name> AUTO_COMPRESS=TRUE` command for every file.
  4. By keeping this in a separate file, our main DAG stays clean and easy to read.

### C. `end_to_end_pipeline.py` (The Master Orchestrator)
- **Purpose**: This is the master DAG that ties the entire ELT process together.
- **How it works**:
  1. **Upload Task**: It imports and calls `upload_files_to_stage()` from `stage_operations.py`.
  2. **Create Bronze Task**: Uses `SQLExecuteQueryOperator` to run a massive `CREATE TABLE IF NOT EXISTS` query, ensuring the raw table exists.
  3. **Load Bronze Task**: Uses `SQLExecuteQueryOperator` to run `COPY INTO`. This pulls data out of the Snowflake stage and safely inserts it into the `BRONZE` table.
  4. **dbt Run Task**: Uses `BashOperator` to execute the dbt transformation scripts, turning Bronze into Silver and Gold.
  5. **Dependencies**: At the bottom, `stage_task >> create_bronze_task >> load_bronze_task >> dbt_run_task` uses bitshift operators (`>>`) to define the exact chronological order of execution. If one task fails, the next will not start.

---

## 4. Common Errors & Troubleshooting

### Error 1: Airflow `ImportError: cannot import name 'SnowflakeOperator'`
- **Cause**: The legacy `SnowflakeOperator` was deprecated in recent provider packages.
- **Solution**: Switch to `SQLExecuteQueryOperator` from `airflow.providers.common.sql.operators.sql`. (Change `snowflake_conn_id=` to `conn_id=`).

### Error 2: dbt Runtime Error `Could not find profile named 'banking_models'`
- **Cause**: In Docker (Astro), if you mount a specific file (`profile.yml`) from Windows, but the file is missing or misspelled on the host, Docker will automatically create a **directory** with that name instead of a file. dbt looks for a file, finds an empty folder, and crashes.
- **Solution**: Mount the folder containing the profile instead (e.g., mapping the whole `banking_dbt` folder to `/usr/local/airflow/dbt`) and use the `--profiles-dir` flag in the bash command to point to that folder.

### Error 3: Bash pathing `No such file or directory` during dbt execution
- **Cause**: You tried to `cd /usr/local/airflow/banking_dbt`, forgetting that inside the container, `docker-compose.override.yml` mapped it to just `/usr/local/airflow/dbt`.
- **Solution**: Always navigate using the *container's* mapped path, not the host's path.

### Error 4: dbt Parser Error `unable to infer all dependencies`
- **Cause**: A notorious hidden bug! This usually happens when SQL files are authored on Windows using `CRLF` (`\r\n`) line endings. The dbt parser running inside the Linux Airflow container expects `LF` (`\n`) and chokes when parsing the `{{ ref() }}` macro followed by a Windows carriage return.
- **Solution**: Run a `dos2unix` script across your `models/` directory to strip out `\r` characters, OR add an explicit `-- depends_on: {{ ref('model') }}` comment block at the very top of your SQL files.

### Error 5: Trying to ingest into Bronze using a dbt model
- **Cause**: Writing a dbt model that runs `SELECT * FROM @stage` instead of using Airflow to run `COPY INTO`.
- **Solution**: Never use dbt for data extraction/loading from raw files. dbt does not track file ingestion metadata. `COPY INTO` natively ignores already-processed files. Always use Airflow for ingestion (Extract/Load) and dbt purely for Transformations!
