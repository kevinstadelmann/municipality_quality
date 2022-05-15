-- PROCEDURE: job.p_log(character varying, integer, integer, character varying)

-- DROP PROCEDURE IF EXISTS job.p_log;

CREATE OR REPLACE PROCEDURE job.p_log(
    IN ip_job_level character varying,
	IN ip_job_id integer,
	IN ip_error_level integer,
	IN ip_text character varying)
LANGUAGE 'plpgsql'
AS $BODY$
begin
	INSERT INTO job.t_log (job_id, error_level, log_message, job_level)
	VALUES
		(ip_job_id,ip_error_level,ip_text, ip_job_level);
    --commit;
end;
$BODY$;
ALTER PROCEDURE job.p_log(character varying, integer, integer, character varying)
    OWNER TO postgres;
