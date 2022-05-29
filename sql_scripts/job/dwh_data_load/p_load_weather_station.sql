-- PROCEDURE: job.p_load_dwh_1(integer)

-- DROP PROCEDURE IF EXISTS job.p_load_dwh_1(integer);

CREATE OR REPLACE PROCEDURE job.p_load_dwh_weather_station(
	IN ip_job_id integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE job_level varchar := 'dwh';
BEGIN

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job start');

-- CREATE tmp Table
CREATE TEMPORARY TABLE tmp_weather_station AS (
SELECT 	"station/location",
		station,
		"station_height_m._a._sea_level",
		CAST(coordinatese as INT),
		CAST(coordinatesn as INT),
		latitude,
		longitude,
	    now() as dwh_create_date,
	    now() as dwh_change_date,
	    'A'
FROM 	stage.t_weather_station
WHERE	station IS NOT NULL
) WITH DATA;

-- *** DELETE *** ---
UPDATE dwh.t_weather_station A
SET     dwh_status = 'D'
    ,   dwh_change_date = now()
WHERE NOT EXISTS (SELECT 1 FROM tmp_weather_station B
                        WHERE B."station/location" = A.station_id)
AND A.dwh_status != 'D'
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Delete step done');

-- *** UPDATE *** ---
UPDATE 	dwh.t_weather_station A
SET 	name 			= station,
		height 		= "station_height_m._a._sea_level",
		e_cntr 		= CAST(coordinatese as INT),
		n_cntr 		= CAST(coordinatesn as INT),
		latitude 		= B.latitude,
		longitude 	= B.longitude,
		dwh_change_date = B.dwh_change_date

FROM 	tmp_weather_station B
WHERE 	B."station/location" = A.station_id
AND 	(A.name 			!= station
		OR A.height 		!= "station_height_m._a._sea_level"
		OR A.e_cntr 		!= CAST(coordinatese as INT)
		OR A.n_cntr 		!= CAST(coordinatesn as INT)
		OR A.latitude 		!= B.latitude
		OR A.longitude 		!= B.longitude
        OR A.dwh_status 	!= 'A'
          )
;
-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Update step done');

-- *** INSERT *** ---
INSERT INTO dwh.t_weather_station
SELECT		"station/location",
			station,
			"station_height_m._a._sea_level",
			CAST(coordinatese as INT),
			CAST(coordinatesn as INT),
			latitude,
			longitude,
			dwh_create_date,
			dwh_change_date,
			'A'
FROM 		tmp_weather_station A
WHERE NOT EXISTS (SELECT 	1
				  FROM 		dwh.t_weather_statistic B
                  WHERE 	B.station_id = A."station/location")
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Insert step done');

-- Drop temp table
DROP TABLE tmp_weather_station;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job end');

END;
$BODY$;



ALTER PROCEDURE job.p_load_dwh_1(integer)
    OWNER TO postgres;
