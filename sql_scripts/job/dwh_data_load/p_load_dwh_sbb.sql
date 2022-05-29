-- PROCEDURE: job.p_load_dwh_sbb(integer)

-- DROP PROCEDURE IF EXISTS job.p_load_dwh_sbb(integer);

CREATE OR REPLACE PROCEDURE job.p_load_dwh_sbb(
	IN ip_job_id integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE job_level varchar := 'dwh';
BEGIN

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job start');

-- CREATE tmp Table
CREATE TEMPORARY TABLE tmp_sbb_stop AS (
SELECT 	bpuic,
		bfs_nummer,
		bezeichnung_offiziell,
		bpvh_verkehrsmittel_text_de
FROM 	stage.t_sbb_stop
WHERE	is_haltestelle 	= '1'
AND		bfs_nummer 		IS NOT NULL
AND		nummer			IS NOT NULL
-- Make sure municipality ID is available
AND		bfs_nummer 		IN (SELECT	municipality_id
							FROM 	dwh.t_municipality)
) WITH DATA;

-- *** DELETE *** ---
UPDATE dwh.t_sbb_stop A
SET     dwh_status = 'D'
    ,   dwh_change_date = now()
WHERE 	NOT EXISTS (SELECT 1 FROM tmp_sbb_stop B
                	WHERE B.bpuic = A.stop_id)
      AND A.dwh_status != 'D'
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Delete step done');

-- *** UPDATE *** ---
UPDATE 	dwh.t_sbb_stop A
SET 	stop_id 		= B.bpuic,
		municipality_id = B.bfs_nummer,
		stop_name 		= B.bezeichnung_offiziell,
		stop_type 		= B.bpvh_verkehrsmittel_text_de,
    	dwh_change_date = now(),
    	dwh_status 		= 'A'
FROM 	tmp_sbb_stop B
WHERE 	A.stop_id = B.bpuic
AND 	(A.stop_name 		!= B.bezeichnung_offiziell
		OR A.stop_type 		!= B.bpvh_verkehrsmittel_text_de
		OR A.municipality_id 	!=  B.bfs_nummer
		OR A.dwh_status 		!= 'A'
        )
;
-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Update step done');

-- *** INSERT *** ---
INSERT INTO dwh.t_sbb_stop
SELECT 	bpuic,
		bfs_nummer,
		bezeichnung_offiziell,
		bpvh_verkehrsmittel_text_de,
		now(),
        now(),
        'A'
FROM 	tmp_sbb_stop A
WHERE	NOT EXISTS (SELECT 	1
					FROM 	dwh.t_sbb_stop B
                    WHERE 	B.stop_id = A.bpuic)
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Insert step done');

-- Drop temp table
DROP TABLE tmp_sbb_stop;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job end');

END;
$BODY$;


ALTER PROCEDURE job.p_load_dwh_sbb(integer)
    OWNER TO postgres;
