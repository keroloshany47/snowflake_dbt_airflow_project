from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DBT project directory
DBT_PROJECT_DIR = "/home/kerolos-hani/DBT_Core/Airflow_DBT_Project/airflow_dbt_snowflake_project"

# Define DAG
with DAG(
    'dbt_workflow',
    default_args=default_args,
    description='Run dbt models using dbt Core',
    schedule='@daily',
    catchup=False,
) as dag:

    # Task 1: Run dbt models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run'
    )

    # Task 2: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test'
    )

    # Set task dependencies
    dbt_run >> dbt_test
