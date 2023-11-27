from datetime import datetime, timedelta
from airflow import DAG
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

def load_bq_raw():
    print("Python Task")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date':  airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'dag_SFCrimes_DimFact',
    default_args=default_args,
    description='DAG to load data from curated Layer to Dimensions & facts',
    schedule_interval=None,
    tags=["SF_crimes"]
)

# Create a PythonOperator task
pre_dbt_task = PythonOperator(
    task_id='pre_dbt_task',
    python_callable=load_bq_raw,
    dag=dag,
)

project_dir = Variable.get("dbt_dir")
tags_to_run = ["tbl_curated_crime"]
# Define the dbt run tasks using BashOperator for each tag
for tag in tags_to_run:
    # run_dbt_command = f'dbt run --tag {tag} --project-dir {project_dir}'
    run_dbt_command = f'dbt run --select "tag:{tag}" --profiles-dir {project_dir}/profiles --project-dir {project_dir}'

    run_dbt_task = BashOperator(
        task_id=f'run_dbt_with_tag_{tag}',
        bash_command=run_dbt_command,
        dag=dag,
    )

    # Set the execution order between tasks
    if tag != tags_to_run[0]:
        run_dbt_task.set_upstream(prev_task)

    prev_task = run_dbt_task

pre_dbt_task >> prev_task
