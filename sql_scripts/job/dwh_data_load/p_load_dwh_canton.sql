-- PROCEDURE: job.p_load_dwh_canton(integer)

-- DROP PROCEDURE IF EXISTS job.p_load_dwh_canton(integer);

CREATE OR REPLACE PROCEDURE job.p_load_dwh_canton(
	IN ip_job_id integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE job_level varchar := 'dwh';
BEGIN

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job start');

-- CREATE tmp Table
CREATE TEMPORARY TABLE tmp_canton AS (
SELECT	distinct SBB.kantonsnum
                , gdekt
                , gdektna
FROM	stage.t_municipality_district	MUN
INNER	JOIN stage.t_sbb_stop			SBB
ON		SBB.kantonskuerzel = MUN.gdekt
) WITH DATA;

-- *** DELETE *** ---
UPDATE dwh.t_canton A
SET     dwh_status 		= 'D'
    ,   dwh_change_date = now()
WHERE NOT EXISTS (SELECT 	1
				  FROM 		tmp_canton B
                  WHERE 	B.kantonsnum = A.canton_id)
      AND A.dwh_status != 'D'
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Delete step done');

-- *** UPDATE *** ---
UPDATE 	dwh.t_canton A
SET  	canton_name_short 	= gdekt,
       	canton_name 		= gdektna,
    	dwh_change_date		= now(),
    	dwh_status 			= 'A'
FROM 	tmp_canton B
WHERE 	B.kantonsnum = A.canton_id
AND 	(canton_name_short	!= gdekt
       	OR canton_name		!= gdektna
        OR A.dwh_status 	!= 'A'
         )
;
-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Update step done');

-- *** INSERT *** ---
INSERT INTO dwh.t_canton
SELECT 		kantonsnum,
			gdekt,
			gdektna,
			now(),
			now(),
			'A'
FROM tmp_canton A
WHERE NOT EXISTS (SELECT 	1
				  FROM 		dwh.t_canton B
                  WHERE 	B.canton_id= A.kantonsnum)
;


-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Insert step done');

-- Drop temp table
DROP TABLE tmp_canton;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job end');

END;
$BODY$;



ALTER PROCEDURE job.p_load_dwh_canton(integer)
    OWNER TO postgres;



