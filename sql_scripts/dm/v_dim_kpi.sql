-- View: dm.v_dim_kpi

-- DROP VIEW dm.v_dim_kpi;

CREATE OR REPLACE VIEW dm.v_dim_kpi
AS

SELECT A.kpi_id
	, A.kpi_name
	FROM dwh.t_kpi A
	WHERE A.dwh_status = 'A'
;

ALTER TABLE dm.v_dim_kpi
    OWNER TO postgres;