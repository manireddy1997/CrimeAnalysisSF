select
      row_id as RowID,
      incident_id as IncidentID,
      incident_number as IncidentNumber,
      report_type_code as ReportType,
      incident_code as IncidentCode,
      police_district as PoliceDistrict,
      analysis_neighborhood as AnalysisNeighborhood,
      intersection as Intersection,
      incident_datetime as IncidentTimeStamp,
      report_datetime as ReportTimeStamp,
      filed_online as OnlineIncident,
      point as PointID,
 from `hakoona-matata-298704.SFCrimeData.tbl_curated_crime`