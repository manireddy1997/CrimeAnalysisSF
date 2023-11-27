select incident_datetime as DateTime,
       EXTRACT(Year FROM incident_datetime) AS Year,
       EXTRACT(month FROM incident_datetime) AS Month,
       FORMAT_DATE('%d', DATE(incident_datetime)) AS Date,
       EXTRACT(WEEK FROM incident_datetime) AS week_number, 
       FORMAT_DATE('%A', incident_datetime) AS week_name,
       FORMAT_DATE('%B', incident_datetime) AS month_name,
       from (

        select incident_datetime from `hakoona-matata-298704.SFCrimeData.tbl_curated_crime`
        union distinct
        select report_datetime from `hakoona-matata-298704.SFCrimeData.tbl_curated_crime` order by 1
)