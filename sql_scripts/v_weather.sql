-- View: dwh.v_weather

-- DROP VIEW dwh.v_weather;

CREATE OR REPLACE VIEW dwh.v_weather
AS

WITH distance
AS
(
SELECT A.municipality_id
, B.station_id
, sqrt((A.e_cntr - B.e_cntr)^2 + (A.n_cntr - B.n_cntr)^2)	"distance"
	FROM dwh.t_municipality A
		CROSS JOIN dwh.t_weather_station B
	WHERE B.height < 1000
	    AND A.dwh_status = 'A'
	    AND B.dwh_status = 'A'
),

distance_rank
AS
(
SELECT municipality_id
, station_id
, distance
, ROW_NUMBER() OVER(PARTITION BY municipality_id ORDER BY distance)		"rank"
FROM distance
),

distance_max
AS
(
SELECT municipality_id
, sum(distance)		"max_distance"
FROM distance_rank
WHERE rank < 4
GROUP BY municipality_id
),

weight
AS
(
SELECT A.municipality_id
, A.station_id
, (1-(A.distance / M.max_distance)) / 2		"weight"
FROM distance_rank A
INNER JOIN distance_max M
	ON M.municipality_id = A.municipality_id

WHERE A.rank < 4
),

stats_year
AS
(	SELECT A.station_id
			, extract(year from A.date)									"year"
			--, SUM(CASE WHEN A.sunshine_min >= 180 THEN 1 ELSE 0 END)	"sunshine_day"
			--, SUM(CASE WHEN A.rain_mm >= 0.1 THEN 1 ELSE 0 END)		"rain_day"
			, SUM(COALESCE(A.sunshine_min,0)/60)						"sunshine_hours"
			, SUM(COALESCE(A.rain_mm,0))								"rain_mm"
	FROM dwh.t_weather_statistic A
		WHERE A.dwh_status = 'A'
			AND A.date > '2013-12-31'
			AND A.date < '2022-01-01'
	GROUP BY A.station_id
			, extract(year from date)
),

stats_avg
AS
(
	SELECT A.station_id
		 , avg(A.sunshine_hours)			"sunshine_hours"
		 , avg(A.rain_mm)					"rain_mm"
	FROM stats_year A
	GROUP BY A.station_id
),

base
AS
(
	SELECT A.municipality_id
		, SUM(B.sunshine_hours * A.weight)		"sunshine_hours"
		, SUM(B.rain_mm * A.weight)				"rain_mm"
	FROM weight A
	INNER JOIN stats_avg B
		ON B.station_id = A.station_id
	GROUP BY A.municipality_id
)

SELECT A.municipality_id
	, 3                                                          "kpi_id"
	, ROUND(CAST(A.sunshine_hours AS numeric),0)				"kpi_value"
FROM base A

UNION ALL

SELECT A.municipality_id
	, 4                                                          "kpi_id"
	, ROUND(CAST(A.rain_mm AS numeric),0)						"kpi_value"
FROM base A;

ALTER TABLE dwh.v_weather
    OWNER TO postgres;




