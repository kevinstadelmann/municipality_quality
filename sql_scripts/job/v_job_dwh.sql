-- View: job.v_job_dwh

-- DROP VIEW job.v_job_dwh;

CREATE OR REPLACE VIEW job.v_job_dwh
 AS
 SELECT t_job_dwh.job_id,
    t_job_dwh.job_name,
    t_job_dwh.run_order,
    t_job_dwh.airflow_connection_db,
    t_job_dwh.proc_name,
    t_job_dwh.db_schema
   FROM job.t_job_dwh
  WHERE t_job_dwh.job_active = true;

ALTER TABLE job.v_job_dwh
    OWNER TO postgres;
