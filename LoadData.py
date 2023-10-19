# !/usr/bin/env python
# def download_data_directAPI(url,destination,columns):
#     import urllib.request, json
#     import pandas as pd
#     from datetime import datetime
#
#     with urllib.request.urlopen(url) as data_obj:
#         data = json.loads(data_obj.read().decode())
#     df = pd.DataFrame(data)
#     filtered_df = df[df.columns[df.columns.str.lower().isin(map(str.lower, columns))]]
#     now = datetime.now()
#     tail = now.strftime("%Y%m%d")
#     dest_file = destination.replace('.csv',tail+'.csv')
#     filtered_df.to_csv(dest_file,sep=',',index=False)
#     return True
def download_data(destination,columns):
    import pandas as pd
    from sodapy import Socrata
    from datetime import datetime

    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata("data.sfgov.org", None)

    # Example authenticated client (needed for non-public datasets):
    # client = Socrata(data.sfgov.org,
    #                  MyAppToken,
    #                  username="user@example.com",
    #                  password="AFakePassword")

    results = client.get("wg3w-h783", limit=2000)
    df = pd.DataFrame.from_records(results)
    filtered_df = df[df.columns[df.columns.str.lower().isin(map(str.lower, columns))]]
    now = datetime.now()
    tail = now.strftime("%Y%m%d")
    dest_file = destination.replace('.csv', tail + '.csv')
    filtered_df.to_csv(dest_file, sep=',', index=False)
    return True

def bq_load_raw(source_file,project_id,dataset_id,table_id):
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)


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

#### Download Data and save in local file #########
url = 'https://data.sfgov.org/resource/wg3w-h783.json'
destination = './data/sfcrimes.csv'
columns = ['incident_datetime','incident_date','incident_time','incident_year','incident_day_of_week','report_datetime','row_id','incident_id','incident_number','report_type_code','report_type_description','incident_code','incident_category','incident_subcategory','incident_description','resolution','police_district','filed_online','cad_number','intersection','cnn','analysis_neighborhood','supervisor_district','supervisor_district_2012','latitude','longitude','point']
# download_data(destination,columns)

#### Load file to GCP  #########

project = 'hakoona-matata-298704'
dataset = 'SFCrimeData'
table = 'tbl_raw_sf_crime'
file = './data/sfcrimes20231018.csv'
# bq_load_raw(source_file=file,project_id=project,dataset_id=dataset,table_id=table)

