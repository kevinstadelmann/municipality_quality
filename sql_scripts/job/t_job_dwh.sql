-- Table: job.t_job_dwh

DROP TABLE IF EXISTS job.t_job_dwh;

CREATE TABLE IF NOT EXISTS job.t_job_dwh
(
    job_id integer NOT NULL,
    job_name character varying(255),
    job_active boolean NOT NULL,
    run_order integer NOT NULL,
    airflow_connection_db character varying(255),
    proc_name character varying(255),
    db_schema character varying(50),

    create_user character varying(255) NOT NULL DEFAULT current_user,
    create_date timestamp with time zone NOT NULL DEFAULT now(),
    change_user character varying(255) NOT NULL DEFAULT current_user,
    change_date timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT pk_t_job_dwh PRIMARY KEY (job_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS job.t_job_dwh
    OWNER to postgres;


INSERT INTO job.t_job_dwh(
	job_id, job_name, job_active, run_order, airflow_connection_db, proc_name, db_schema)
	VALUES (1, '01_load_weather_statistic', '1', 100, 'aws_rds_postgres_dataocean', 'p_load_dwh_1', 'job'),
	       (2, '02_load_sbb', '1', 100, 'aws_rds_postgres_dataocean', 'p_load_dwh_sbb', 'job');