-- Table: job.t_job

DROP TABLE IF EXISTS job.t_job;

CREATE TABLE IF NOT EXISTS job.t_job
(
    job_id integer NOT NULL,
    job_name character varying(255),
    job_active boolean NOT NULL,
    file_type character varying(255),
    file_url character varying(255),
    file_encoding character varying(50),
    file_sep character varying(50),
    file_tab character varying(50),
    file_skiprows integer,
    airflow_connection_s3 character varying(255),
    airflow_connection_db character varying(255),
    table_name character varying(255),
    db_schema character varying(50),
    aws_s3_bucket_name character varying(255),
    aws_s3_bucket_path character varying(255),

    create_user character varying(255) NOT NULL DEFAULT current_user,
    create_date timestamp with time zone NOT NULL DEFAULT now(),
    change_user character varying(255) NOT NULL DEFAULT current_user,
    change_date timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT pk_t_job PRIMARY KEY (job_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS job.t_job
    OWNER to postgres;


INSERT INTO job.t_job(
	job_id, job_name, job_active, file_type
	, file_url
	, file_encoding, file_sep, file_tab, file_skiprows
	, airflow_connection_s3, airflow_connection_db
	, table_name, db_schema, aws_s3_bucket_name, aws_s3_bucket_path)
	VALUES (1, '01_load_sbb_stop', '1', 'json'
			, 'https://data.sbb.ch/api/v2/catalog/datasets/dienststellen-gemass-opentransportdataswiss/exports/json?limit=100000&offset=0&timezone=UTC&apikey=8a89815df44a86552121399631d28f4472d3a150a9f1d6686ceb5c09'
			, 'UTF-8', ',', NULL, 0
			, 'aws_s3_dataocean-datalake', 'aws_rds_postgres_dataocean'
			, 't_sbb_stop', 'stage', 'dataocean-datalake', '01_SBB_Stop/sbb_stop_'
			),
			(2, '02_load_weather_station', '1', 'csv'
			, 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/liste-download-nbcn-d.csv'
			, 'ISO-8859-1', ';', NULL, 0
			 , 'aws_s3_dataocean-datalake', 'aws_rds_postgres_dataocean'
			, 't_weather_station', 'stage', 'dataocean-datalake', '02_Weather_Station/weather_station_main_'
			),
			(3, '03_load_weather_statistic', '1', 'csv'
			, 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_*'
			, 'ISO-8859-1', ';' , NULL, 0
			 , 'aws_s3_dataocean-datalake', 'aws_rds_postgres_dataocean'
			, 't_weather_statistic', 'stage', 'dataocean-datalake', '03_Weather_Statistic/weather_statistic_'
			),
			(4, '04_load_municipality_zipcode', '1', 'xlsx'
			, 'https://dam-api.bfs.admin.ch/hub/api/dam/assets/7226419/master'
			, 'ISO-8859-1', NULL, 'PLZ4', 0
			, 'aws_s3_dataocean-datalake', 'aws_rds_postgres_dataocean'
			, 't_municipality_zipcode', 'stage', 'dataocean-datalake', '04_Municipality_Zipcode/municipality_zipcode_'
			),
			(5, '05_load_municipality_statistic', '1', 'xlsx'
			, 'https://dam-api.bfs.admin.ch/hub/api/dam/assets/15864450/master'
			, 'ISO-8859-1', NULL, 'T21.3.1', 5
			, 'aws_s3_dataocean-datalake', 'aws_rds_postgres_dataocean'
			, 't_municipality_statistic', 'stage', 'dataocean-datalake', '05_Municipality_Statistic/municipality_statistic_'
			),
			(6, '06_load_municipality_district', '1', 'xlsx'
			, 'https://dam-api.bfs.admin.ch/hub/api/dam/assets/22304854/master'
			, 'ISO-8859-1', NULL, 'GDE', 0
			, 'aws_s3_dataocean-datalake', 'aws_rds_postgres_dataocean'
			, 't_municipality_district', 'stage', 'dataocean-datalake', '06_Municipality_District/municipality_district_'
			),
			(7, '07_load_municipality_location', '1', 'xlsx'
			, 'https://dam-api.bfs.admin.ch/hub/api/dam/assets/21224784/master'
			, 'ISO-8859-1', NULL, 'g1g22', 0
			, 'aws_s3_dataocean-datalake', 'aws_rds_postgres_dataocean'
			, 't_municipality_location', 'stage', 'dataocean-datalake', '07_Municipality_Location/municipality_location_'
			);