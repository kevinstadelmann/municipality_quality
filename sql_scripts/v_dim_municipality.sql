-- View: dm.v_dim_municipality

-- DROP VIEW dm.v_dim_municipality;

CREATE OR REPLACE VIEW dm.v_dim_municipality
AS

SELECT A.municipality_id
, A.name			"municipality_name"
, A.district_id
, B.district_name
, A.canton_id
, C.canton_name_short
, C.canton_name
	FROM dwh.t_municipality A
	LEFT OUTER JOIN dwh.t_district B
		ON B.district_id = A.district_id
		AND B.dwh_status = 'A'
	LEFT OUTER JOIN dwh.t_canton C
		ON C.canton_id = A.canton_id
		AND C.dwh_status = 'A'
	WHERE A.dwh_status = 'A'

;

ALTER TABLE dm.v_dim_municipality
    OWNER TO postgres;