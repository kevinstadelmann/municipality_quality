-- SBB public transportation stops
CREATE OR REPLACE VIEW dwh.v_sbb_stop AS
SELECT		STA.municipality_id,
			'density_stop' AS kpi_density_stop,
			COALESCE(COUNT(STO.stop_id) / STA.municipality_area_sqkm, 0) as kpi_value_density_stop

FROM 		dwh.t_municipality_statistic	STA

LEFT JOIN	DWH.t_sbb_stop					STO
ON			STO.municipality_id = STA.municipality_id

GROUP BY	STA.municipality_id,
			STA.municipality_area_sqkm;

-- political orientation

CREATE OR REPLACE VIEW dwh.v_municipality  AS
SELECT	municipality_id,
		'political_orientation' as kpi_political_orientation,
(COALESCE(political_party_fdp_percent,0) 	* 0.7+
COALESCE(political_party_cvp_percent,0) 	* 0.5+
COALESCE(political_party_sp_percent,0) 		* 0.2+
COALESCE(political_party_bdp_percent,0)		* 0.5+
COALESCE(political_party_glp_percent,0)		* 0.4+
COALESCE(political_party_evp_percent,0)		* 0.5+
COALESCE(political_party_svp_percent,0)		* 0.9+
COALESCE(political_party_gps_percent,0)		* 0.1+
COALESCE(political_party_small_right_wing_percent,0)	* 1.0)/100 as kpi_value_polticial_direction

FROM	dwh.t_municipality_statistic