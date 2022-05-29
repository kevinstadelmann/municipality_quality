-- PROCEDURE: job.p_load_dwh_municipality(integer)

-- DROP PROCEDURE IF EXISTS job.p_load_dwh_municipality(integer);

CREATE OR REPLACE PROCEDURE job.p_load_dwh_municipality(
	IN ip_job_id integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE job_level varchar := 'dwh';
BEGIN

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job start');

-- CREATE tmp Table
CREATE TEMPORARY TABLE tmp_municipality AS (
SELECT 	gmdnr,
		gmdname,
		bznr,
		ktnr,
		e_cntr,
		n_cntr,
		now() AS dwh_create_date,
        now() AS dwh_change_date,
        'A' AS dwh_status
FROM 	stage.t_municipality_location
) WITH DATA;

-- *** DELETE *** ---
UPDATE  dwh.t_municipality A
SET     dwh_status = 'D',
        dwh_change_date = now()
WHERE   NOT EXISTS (SELECT  1
                    FROM    tmp_municipality B
                    WHERE   B.gmdnr = A.municipality_id)
AND A.dwh_status != 'D'
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Delete step done');

-- *** UPDATE *** ---
UPDATE dwh.t_municipality A
SET name 				= B.gmdname
    , district_id 		= B.bznr
    , canton_id 		= B.ktnr
    , e_cntr 			= B.e_cntr
    , n_cntr 			= B.n_cntr
    , dwh_change_date	= now()
    , dwh_status 		= 'A'
FROM tmp_municipality B
WHERE B.gmdnr = A.municipality_id
      AND 	(A.name != B.gmdname
    		OR A.district_id 		!= B.bznr
    		OR A.canton_id 		!= B.ktnr
    		OR A.e_cntr 			!= B.e_cntr
    		OR A.n_cntr 			!= B.n_cntr
            OR A.dwh_status != 'A'
          )
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Update step done');

-- *** INSERT *** ---
INSERT INTO dwh.t_municipality
SELECT 	gmdnr,
		gmdname,
		bznr,
		ktnr,
		e_cntr,
		n_cntr,
		now(),
        now(),
        'A'
FROM 	tmp_municipality A
WHERE NOT EXISTS (SELECT 	1
				  FROM 		dwh.t_municipality B
                  WHERE 	B.municipality_id = A.gmdnr)
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Insert step done');

-- Drop temp table
DROP TABLE tmp_municipality;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job end');

END;
$BODY$;



ALTER PROCEDURE job.p_load_dwh_municipality(integer)
    OWNER TO postgres;



