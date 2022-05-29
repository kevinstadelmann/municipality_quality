-- PROCEDURE: job.p_load_dwh_1(integer)

-- DROP PROCEDURE IF EXISTS job.p_load_dwh_1(integer);

CREATE OR REPLACE PROCEDURE job.p_load_dwh_district(
	IN ip_job_id integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE job_level varchar := 'dwh';
BEGIN

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job start');

-- CREATE tmp Table
CREATE TEMPORARY TABLE tmp_district AS (
SELECT	distinct MUN.gdebznr
		, MUN.gdebzna
		, now() as dwh_create_date
		, now() as dwh_change_date
		, 'A'
FROM	stage.t_municipality_district MUN
) WITH DATA;

-- *** DELETE *** ---
UPDATE dwh.t_district A
SET     dwh_status = 'D'
    ,   dwh_change_date = now()
WHERE NOT EXISTS (SELECT 	1
				  FROM 		tmp_district B
                  WHERE 	B.gdebznr = A.district_id)
AND A.dwh_status != 'D'

;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Delete step done');

-- *** UPDATE *** ---
UPDATE dwh.t_district A
SET 	district_name = gdebzna
    	, dwh_change_date = now()
    	, dwh_status = 'A'
FROM 	tmp_district B
WHERE 	B.gdebznr = A.district_id
AND 	(A.district_name != B.gdebzna
        OR A.dwh_status != 'A'
          )
;
-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Update step done');

-- *** INSERT *** ---
INSERT INTO dwh.t_district
SELECT 		gdebznr,
			gdebzna,
			now(),
			now(),
			'A'

FROM tmp_district A
WHERE NOT EXISTS (SELECT 	1
				  FROM 		dwh.t_district B
                  WHERE 	B.district_id = A.gdebznr)
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Insert step done');

-- Drop temp table
DROP TABLE tmp_district;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job end');

END;
$BODY$;



ALTER PROCEDURE job.p_load_dwh_1(integer)
    OWNER TO postgres;
