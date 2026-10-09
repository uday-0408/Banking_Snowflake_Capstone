
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
)
from cosmos.config import RenderConfig


# --------------------------------------------------
# Paths inside the Airflow Docker container
# Replace these with your actual paths.
# --------------------------------------------------

DBT_PROJECT_PATH = "/usr/local/airflow/dbt/banking_models"
DBT_PROFILES_PATH = "/usr/local/airflow/dbt/profiles.yml"
DBT_EXECUTABLE_PATH = "/usr/local/bin/dbt"


# --------------------------------------------------
# Select your existing dbt profile
# --------------------------------------------------

profile_config = ProfileConfig(
    profile_name="banking_models",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH,
)


# --------------------------------------------------
# Create the Airflow DAG
# --------------------------------------------------

with DAG(
    dag_id="cosmos_dbt_pipeline",
    start_date=datetime(2026, 10, 9),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,
    },
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    # Cosmos converts dbt resources into Airflow tasks
    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",

        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),

        profile_config=profile_config,

        # Used when Airflow executes the dbt tasks
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),

        # Used when Cosmos discovers dbt resources
        render_config=RenderConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),

        default_args={
            "retries": 1,
        },
    )

    finish = EmptyOperator(
        task_id="finish",
    )

    # Pipeline dependencies
    start >> dbt_transformations >> finish
