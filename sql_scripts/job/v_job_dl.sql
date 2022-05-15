-- View: job.v_job_dl

-- DROP VIEW job.v_job_dl;

CREATE OR REPLACE VIEW job.v_job_dl
 AS
 SELECT t_job_dl.job_id,
    t_job_dl.job_name,
    t_job_dl.file_type,
    t_job_dl.file_url,
    t_job_dl.file_encoding,
    t_job_dl.file_sep,
    t_job_dl.file_tab,
    t_job_dl.file_skiprows,
    t_job_dl.airflow_connection_s3,
    t_job_dl.airflow_connection_db,
    t_job_dl.table_name,
    t_job_dl.db_schema,
    t_job_dl.aws_s3_bucket_name,
    REPLACE((((t_job_dl.aws_s3_bucket_path::text || to_char(now()::timestamp without time zone, 'yyyy_mm_dd_HH24_MI_ss'::text)) || '.'::text) || t_job_dl.file_type::text), '.xlsx', '.csv') AS aws_s3_bucket_path
   FROM job.t_job_dl
  WHERE t_job_dl.job_active = true;

ALTER TABLE job.v_job_dl
    OWNER TO postgres;
