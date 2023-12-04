from datetime import datetime, timedelta
from airflow import DAG
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from utils.adminvars import set_variable


# Define the Python function to be executed
def download_data(**kwargs):
    import pandas as pd
    from sodapy import Socrata
    from datetime import datetime, timedelta

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata("data.sfgov.org", None)

    # Example authenticated client (needed for non-public datasets):
    # client = Socrata(data.sfgov.org,
    #                  MyAppToken,
    #                  username="user@example.com",
    #                  password="AFakePassword")
    today = datetime.now() - timedelta(days=2)
    filters = "incident_date=='" + today.strftime("%Y-%m-%d") + "T00:00:00.000'"
    results = client.get("wg3w-h783", where=filters)
    columns = ['incident_datetime','incident_date','incident_time','incident_year','incident_day_of_week','report_datetime','row_id','incident_id','incident_number','report_type_code','report_type_description','incident_code','incident_category','incident_subcategory','incident_description','resolution','police_district','filed_online','cad_number','intersection','cnn','analysis_neighborhood','supervisor_district','supervisor_district_2012','latitude','longitude','point']
    df = pd.DataFrame.from_records(results)
    filtered_df = df[df.columns[df.columns.str.lower().isin(map(str.lower, columns))]]
    now = datetime.now()
    tail = now.strftime("%Y%m%d")
    dest_file = Variable.get("SFcrimes_RawFiles")+'/SFcrimes_'+tail+'.csv'
    filtered_df.to_csv(dest_file, sep=',', index=False)
    set_variable(key='SFcrimes_'+tail, value='SFcrimes_'+tail+'.csv')


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date':  airflow.utils.dates.days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'dag_SFCrimes_data_download',
    default_args=default_args,
    description='DAg to extract data from SF.gov website and load it to a file',
    schedule_interval='0 5 * * *',
    tags=["SF_crimes"]
)

# Create a PythonOperator task
download_data_task = PythonOperator(
    task_id='download_data_task',
    python_callable=download_data,
    dag=dag,
)


trigger_successor_task = TriggerDagRunOperator(
    task_id="trigger_successor_task",
    trigger_dag_id="dag_SFCrimes_data_bqload",
    dag=dag,
)

download_data_task >> trigger_successor_task
