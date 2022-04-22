-- PROCEDURE: job.p_log(integer, integer, character varying)

-- DROP PROCEDURE IF EXISTS job.p_log(integer, integer, character varying);

CREATE OR REPLACE PROCEDURE job.p_log(
	IN ip_job_id integer,
	IN ip_error_level integer,
	IN ip_text character varying)
LANGUAGE 'plpgsql'
AS $BODY$
begin
	INSERT INTO job.t_log (job_id, error_level, log_message)
	VALUES
		(ip_job_id,ip_error_level,ip_text);
    --commit;
end;
$BODY$;
ALTER PROCEDURE job.p_log(integer, integer, character varying)
    OWNER TO postgres;
