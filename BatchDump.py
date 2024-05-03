import json
import pandas as pd

file = './data/batchdump_APR29.csv'

def fileprocessing(file):
    df = pd.read_csv(file, header=True)
    factcolumns = ['Row ID', 'Incident ID', 'Incident Number', 'Report Type Code', 'Incident Code', 'Police District',
                   'Analysis Neighborhood', 'Intersection', 'Incident Datetime', 'Report Datetime', 'Filed Online',
                   'Point', 'Resolution']
    incidentreportcolumns = ['Incident Code', 'Incident Category', 'Incident Subcategory', 'Incident Description']
    reportcolumns = ['Report Type Code', 'Report Type Description']
    locationcolumns = ['Point', 'Latitude', 'Longitude']
    curatedcolumns = ['Incident Datetime', 'Incident Date', 'Report Datetime', 'Row ID', 'Incident ID',
                      'Incident Number', 'Report Type Code', 'Report Type Description', 'Incident Code',
                      'Incident Category', 'Incident Subcategory', 'Incident Description', 'Resolution',
                      'Police District', 'Filed Online', 'CAD Number', 'Intersection', 'CNN', 'Analysis Neighborhood',
                      'Supervisor District', 'Supervisor District 2012', 'Latitude', 'Longitude', 'Point']

    Factdf = df[factcolumns]
    Factdf.columns = ["RowID", "IncidentID", "IncidentNumber", "ReportType", "IncidentCode", "PoliceDistrict",
                      "AnalysisNeighborhood", "Intersection", "IncidentTimeStamp", "ReportTimeStamp", "OnlineIncident",
                      "PointID", "Resolution"]
    Reportdf = df[reportcolumns]
    Reportdf.columns = ["report_type_code", "report_type_description"]
    IncidentReportdf = df[incidentreportcolumns]
    IncidentReportdf.columns = ["incident_code", "incident_category", "incident_subcategory", "incident_description"]
    Datedf = pd.concat([df['Incident Datetime'], df['Report Datetime']], ignore_index=True).to_frame()
    Datedf.columns = ['DateTime']
    Locationdf = df[locationcolumns]
    Locationdf.columns = ['PointID', 'Latitude', 'Longitude']
    Curateddf = df[curatedcolumns]
    Curateddf.columns = ["incident_datetime", "incident_date", "report_datetime", "row_id", "incident_id",
                         "incident_number", "report_type_code", "report_type_description", "incident_code",
                         "incident_category", "incident_subcategory", "incident_description", "resolution",
                         "police_district", "onlineincident", "cad_number", "intersection", "cnn",
                         "analysis_neighborhood", "supervisor_district", "supervisor_district_2012", "latitude",
                         "longitude", "point"]
    Reportdf = Reportdf.drop_duplicates()
    IncidentReportdf = IncidentReportdf.drop_duplicates()
    Locationdf = Locationdf.drop_duplicates()
    Datedf = Datedf.drop_duplicates()
    Datedf['DateTime'] = pd.to_datetime(Datedf['DateTime'])
    Datedf['Date'] = Datedf['DateTime'].dt.date
    Datedf['Year'] = Datedf['DateTime'].dt.year
    Datedf['Month'] = Datedf['DateTime'].dt.month
    Datedf['Day'] = Datedf['DateTime'].dt.day
    Datedf['Weeknumber'] = Datedf['DateTime'].dt.isocalendar().week
    Datedf['Weekday'] = Datedf['DateTime'].dt.strftime('%A')
    Datedf['MonthName'] = Datedf['DateTime'].dt.strftime('%B')
    Datedf['Weeknumber'] = Datedf['Weeknumber'].astype(int)
    Curateddf['incident_datetime'] = pd.to_datetime(Curateddf['incident_datetime'])
    Curateddf['incident_date'] = pd.to_datetime(Curateddf['incident_date'])
    Curateddf['report_datetime'] = pd.to_datetime(Curateddf['report_datetime'])
    Curateddf['cad_number'] = Curateddf['cad_number'].fillna(9999).astype(int)
    Curateddf['cnn'] = Curateddf['cnn'].fillna(9999).astype(int)
    Curateddf['supervisor_district'] = Curateddf['supervisor_district'].fillna(9999).astype(int)
    Curateddf['supervisor_district_2012'] = Curateddf['supervisor_district_2012'].fillna(9999).astype(int)
    Curateddf["onlineincident"] = Curateddf["onlineincident"].fillna(0).astype(int)
    Curateddf['load_timestamp'] = 'Batch Load'
    Factdf["OnlineIncident"] = Factdf["OnlineIncident"].fillna(0).astype(int)

    return Curateddf,Factdf,Datedf,Locationdf,IncidentReportdf,Reportdf

def DataLoad(data_file):
    Curated,Fact,Date,Location,IncidentReport,Report = fileprocessing(file=data_file)

    from google.oauth2 import service_account
    project_id = 'hakoona-matata-298704'
    service_account_file = '/Users/mani/Downloads/hakoona-matata-298704-ae95f0e3a754.json'
    credentials = service_account.Credentials.from_service_account_file(service_account_file)

    curated_table = 'SFCrimeData.tbl_curated_crime'
    Curated.to_gbq(curated_table, project_id=project_id, if_exists='replace', credentials=credentials,
                     table_schema=[{"name": "incident_date", "type": "DATE"}])

    date_table = 'SFCrimeData.tbl_dim_date'
    Date.to_gbq(date_table, project_id=project_id, if_exists='replace', credentials=credentials,
                  table_schema=[{"name": "Date", "type": "DATE"}])

    incidentreport_table = 'SFCrimeData.tbl_dim_incident_report'
    IncidentReport.to_gbq(incidentreport_table, project_id=project_id, if_exists='replace', credentials=credentials)

    report_table = 'SFCrimeData.tbl_dim_report'
    Report.to_gbq(report_table, project_id=project_id, if_exists='replace', credentials=credentials)

    fact_table = 'SFCrimeData.tbl_fact_crime'
    Fact.to_gbq(fact_table, project_id=project_id, if_exists='replace', credentials=credentials)

    location_table = 'SFCrimeData.tbl_dim_location'
    Location.to_gbq(location_table, project_id=project_id, if_exists='replace', credentials=credentials)