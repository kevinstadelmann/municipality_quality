-- View: job.v_job

-- DROP VIEW job.v_job;

CREATE OR REPLACE VIEW job.v_job
 AS
 SELECT t_job.job_id,
    t_job.job_name,
    t_job.file_type,
    t_job.file_url,
    t_job.file_encoding,
    t_job.file_sep,
    t_job.file_tab,
    t_job.file_skiprows,
    t_job.airflow_connection_s3,
    t_job.airflow_connection_db,
    t_job.table_name,
    t_job.db_schema,
    t_job.aws_s3_bucket_name,
    REPLACE((((t_job.aws_s3_bucket_path::text || to_char(now()::timestamp without time zone, 'yyyy_mm_dd_HH24_MI_ss'::text)) || '.'::text) || t_job.file_type::text), '.xlsx', '.csv') AS aws_s3_bucket_path
   FROM job.t_job
  WHERE t_job.job_active = true;

ALTER TABLE job.v_job
    OWNER TO postgres;
