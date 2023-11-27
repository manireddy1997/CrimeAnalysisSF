from datetime import datetime, timedelta
from airflow import DAG
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

def load_bq_raw():
    from google.cloud import bigquery
    from google.oauth2 import service_account

    project_id = Variable.get("project_id")
    dataset_id = Variable.get("SFCrimes_dataset_id")
    table_id = Variable.get("SFCrime_raw_table_id")
    credentials = service_account.Credentials.from_service_account_file(Variable.get("bq_ServiceAccount"))
    client = bigquery.Client(project=project_id, credentials=credentials)
    source_file = Variable.get("SFcrimes_RawFiles")+'/SFcrimes_'+datetime.now().strftime("%Y%m%d%H")+'.csv'

    # Configure the job to automatically detect the schema
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        field_delimiter=',',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

    # Construct a reference to the dataset and table
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)

    # Load the CSV file into the table
    with open(source_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    # Wait for the job to complete
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date':  airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'dag_SFCrimes_data_bqload',
    default_args=default_args,
    description='DAG to load data from file to Bigquery',
    schedule_interval=None,
    tags=["SF_crimes"]
)

# Create a PythonOperator task
load_to_bq_task = PythonOperator(
    task_id='load_to_bq_task',
    python_callable=load_bq_raw,
    dag=dag,
)



load_to_bq_task #>> trigger_successor_task
