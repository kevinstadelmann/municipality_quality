-- View: dm.v_fact_optimal_municipality

-- DROP VIEW dm.v_fact_optimal_municipality;

CREATE OR REPLACE VIEW dm.v_fact_optimal_municipality
AS

WITH stats
AS
(
SELECT * FROM dwh.v_municipality
UNION ALL
SELECT * FROM dwh.v_sbb_stop
UNION ALL
SELECT * FROM dwh.v_weather
)

SELECT B.municipality_id
	, A.persona_id
	, A.kpi_id
	, ROUND(CASE WHEN ABS(B.kpi_value-A.target_value) >= A.max_deviation
			THEN 0
			ELSE 1 - (ABS(B.kpi_value-A.target_value) / A.max_deviation)
			END, 3) 			"rating"
	, ROUND(B.kpi_value,3)		"kpi_value"

FROM dwh.t_persona_kpi A
LEFT OUTER JOIN stats B
	ON B.kpi_id = A.kpi_id
WHERE A.dwh_status = 'A'

;

ALTER TABLE dm.v_fact_optimal_municipality
    OWNER TO postgres;
