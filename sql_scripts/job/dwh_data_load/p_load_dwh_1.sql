-- PROCEDURE: job.p_load_dwh_1(integer)

-- DROP PROCEDURE IF EXISTS job.p_load_dwh_1(integer);

CREATE OR REPLACE PROCEDURE job.p_load_dwh_1(
	IN ip_job_id integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE job_level varchar := 'dwh';
BEGIN

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job start');

-- CREATE tmp Table
CREATE TEMPORARY TABLE tmp_weather_statistic AS (
SELECT	CONCAT("station/location", to_date(date::text, 'YYYYMMDD'))         			"statistic_id",
		"station/location"                                                 				"station_id",
		to_date(date::text, 'YYYYMMDD')                                     			"date",
		CAST((CASE WHEN CAST(gre000d0 AS VARCHAR) = '-' THEN NULL ELSE gre000d0 END) AS DECIMAL(9,4))       "radiation_daily_avg_wsqm",
		CAST((CASE WHEN CAST(hto000d0 AS VARCHAR) = '-' THEN NULL ELSE hto000d0 END) AS DECIMAL(9,4))       "snow_cm",
		CAST((CASE WHEN CAST(nto000d0 AS VARCHAR) = '-' THEN NULL ELSE nto000d0 END) AS DECIMAL(9,4))       "cloud_percent",
		CAST((CASE WHEN CAST(prestad0 AS VARCHAR) = '-' THEN NULL ELSE prestad0 END) AS DECIMAL(5,1)) 	    "air_pressure_hpa",
		CAST((CASE WHEN CAST(rre150d0 AS VARCHAR) = '-' THEN NULL ELSE rre150d0 END) AS DECIMAL(9,4)) 	    "rain_mm",
		CAST((CASE WHEN CAST(sre000d0 AS VARCHAR) = '-' THEN NULL ELSE sre000d0 END) AS DECIMAL(9,4))       "sunshine_min",
		CAST((CASE WHEN CAST(tre200d0 AS VARCHAR) = '-' THEN NULL ELSE tre200d0 END) AS DECIMAL(5,1))       "air_temp_avg_celsius",
		CAST((CASE WHEN CAST(tre200dn AS VARCHAR) = '-' THEN NULL ELSE tre200dn END) AS DECIMAL(5,1))       "air_temp_min_celsius",
		CAST((CASE WHEN CAST(tre200dx AS VARCHAR) = '-' THEN NULL ELSE tre200dx END) AS DECIMAL(5,1))       "air_temp_max_celsius",
		CAST((CASE WHEN CAST(ure200d0 AS VARCHAR) = '-' THEN NULL ELSE ure200d0 END) AS DECIMAL(9,4))  	    "humidity_avg_percent"

FROM	stage.t_weather_statistic
WHERE  extract(year from to_date(date::text, 'YYYYMMDD'))  = extract(year from now())
) WITH DATA;

-- *** DELETE *** ---
UPDATE dwh.t_weather_statistic A
SET     dwh_status = 'D'
    ,   dwh_change_date = now()
WHERE NOT EXISTS (SELECT 1 FROM tmp_weather_statistic B
                        WHERE B.statistic_id = A.statistic_id)
      AND A.dwh_status != 'D'
      AND extract(year from A.date) = extract(year from now())
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Delete step done');

-- *** UPDATE *** ---
UPDATE dwh.t_weather_statistic A
SET radiation_daily_avg_wsqm = B.radiation_daily_avg_wsqm
    , snow_cm = B.snow_cm
    , cloud_percent = B.cloud_percent
    , air_pressure_hpa = B.air_pressure_hpa
    , rain_mm = B.rain_mm
    , sunshine_min = B.sunshine_min
    , air_temp_avg_celsius = B.air_temp_avg_celsius
    , air_temp_min_celsius = B.air_temp_min_celsius
    , air_temp_max_celsius = B.air_temp_max_celsius
    , humidity_avg_percent = B.humidity_avg_percent
    , dwh_change_date = now()
    , dwh_status = 'A'
FROM tmp_weather_statistic B
WHERE B.statistic_id = A.statistic_id
      AND ( A.radiation_daily_avg_wsqm != B.radiation_daily_avg_wsqm
            OR A.snow_cm != B.snow_cm
            OR A.cloud_percent != B.cloud_percent
            OR A.air_pressure_hpa != B.air_pressure_hpa
            OR A.rain_mm != B.rain_mm
            OR A.sunshine_min != B.sunshine_min
            OR A.air_temp_avg_celsius != B.air_temp_avg_celsius
            OR A.air_temp_min_celsius != B.air_temp_min_celsius
            OR A.air_temp_max_celsius != B.air_temp_max_celsius
            OR A.humidity_avg_percent != B.humidity_avg_percent
            OR A.dwh_status != 'A'
          )
;
-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Update step done');

-- *** INSERT *** ---
INSERT INTO dwh.t_weather_statistic
SELECT A.statistic_id
    , A.station_id
    , A.date
    , A.radiation_daily_avg_wsqm
    , A.snow_cm
    , A.cloud_percent
    , A.air_pressure_hpa
    , A.rain_mm
    , A.sunshine_min
    , A.air_temp_avg_celsius
    , A.air_temp_min_celsius
    , A.air_temp_max_celsius
    , A.humidity_avg_percent
	, now()
	, now()
	, 'A'

FROM tmp_weather_statistic A
WHERE NOT EXISTS (SELECT 1 FROM dwh.t_weather_statistic B
                        WHERE B.statistic_id = A.statistic_id)
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Insert step done');

-- Drop temp table
DROP TABLE tmp_weather_statistic;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job end');

END;
$BODY$;



ALTER PROCEDURE job.p_load_dwh_1(integer)
    OWNER TO postgres;



