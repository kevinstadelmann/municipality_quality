-- PROCEDURE: job.p_load_dwh_1(integer)

-- DROP PROCEDURE IF EXISTS job.p_load_dwh_1(integer);

CREATE OR REPLACE PROCEDURE job.p_load_dwh_municipality_statistic(
	IN ip_job_id integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE job_level varchar := 'dwh';
BEGIN

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job start');

-- CREATE tmp Table
CREATE TEMPORARY TABLE tmp_municipality_statistic AS (
SELECT	*
FROM	stage.t_municipality_statistic
WHERE	gemeindecode IS NOT NULL
AND		gemeindename IS NOT NULL
AND		CAST(gemeindecode as INT) IN (SELECT municipality_id FROM dwh.t_municipality)
) WITH DATA

-- Update misbehaving columns: political parties
UPDATE	tmp_municipality_statistic
SET		cvp			= CASE WHEN cvp = '*' 		THEN NULL 	ELSE cvp END,
		"fdp_2)"	= CASE WHEN "fdp_2)" = '*' 	THEN NULL  	ELSE "fdp_2)" END,
		sp			= CASE WHEN sp = '*' 		THEN NULL	ELSE sp END,
		svp			= CASE WHEN svp = '*' 		THEN NULL 	ELSE svp END,
		"evp/csp"	= CASE WHEN "evp/csp" = '*' THEN  NULL 	ELSE "evp/csp" END,
		glp			= CASE WHEN glp = '*' 		THEN NULL 	ELSE glp END,
		bdp			= CASE WHEN bdp = '*' 		THEN NULL 	ELSE bdp END,
		"pda/sol."	= CASE WHEN "pda/sol." = '*' THEN  NULL ELSE "pda/sol." END,
		gps			= CASE WHEN gps = '*' 		THEN  NULL	ELSE gps END,
		kleine_rechtsparteien =
					  CASE WHEN kleine_rechtsparteien = '*' THEN NULL ELSE kleine_rechtsparteien END

-- UPDATE misbehaving columns: working KPIs
UPDATE 	tmp_municipality_statistic
SET		"beschäftigte_total" 	= CASE WHEN "beschäftigte_total" = 'X' THEN NULL ELSE "beschäftigte_total" END,
		"im_1._sektor"			= CASE WHEN "im_1._sektor"	= 'X' THEN NULL ELSE "im_1._sektor" END,
		"im_2._sektor"			= CASE WHEN "im_2._sektor"	= 'X' THEN NULL ELSE "im_2._sektor" END,
		"im_3._sektor"			= CASE WHEN "im_3._sektor"	= 'X' THEN NULL ELSE "im_3._sektor" END,
		"arbeitsstätten_total" = CASE WHEN "arbeitsstätten_total" = 'X' THEN NULL ELSE "arbeitsstätten_total" END,
		"im_1._sektor.1"		= CASE WHEN "im_1._sektor.1"	 = 'X' THEN NULL ELSE "im_1._sektor.1"	END,
		"im_2._sektor.1"		= CASE WHEN "im_2._sektor.1"	 = 'X' THEN NULL ELSE "im_2._sektor.1"	END,
		"im_3._sektor.1"		= CASE WHEN "im_3._sektor.1"	 = 'X' THEN NULL ELSE "im_3._sektor.1"	END,
		"sozialhilfequote"		= CASE WHEN "sozialhilfequote" 	 = 'X' THEN NULL ELSE "sozialhilfequote"	END



-- *** DELETE *** ---
UPDATE dwh.t_municipality_statistic A
SET     dwh_status = 'D'
    ,   dwh_change_date = now()
WHERE 	NOT EXISTS (SELECT 	1
					FROM 	tmp_municipality_statistic B
                    WHERE 	CAST(gemeindecode as INTEGER) = municipality_id)
AND 	A.dwh_status != 'D'
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Delete step done');

-- *** UPDATE *** ---
UPDATE 	dwh.t_municipality_statistic A
SET		population_count 				= CAST(einwohner as INTEGER),
		population_density_sqkm			= CAST("bevölkerungs-dichte_pro_km²" as INTEGER),
		population_foreigner_percent	= CAST("ausländer_in_%%" as DECIMAL(5,2)),
		age_0_19_percent				= CAST("0-19_jahre" as DECIMAL(5,2)),
		age_20_64_percent				= CAST("20-64_jahre" as DECIMAL(5,2)),
		age_65_plus_percent				= CAST("65_jahre_und_mehr" as DECIMAL(5,2)),
		marriage_rate_per_1000			= CAST(rohe_heiratssziffer as DECIMAL(5,2)),
		divorce_rate_per_1000			= CAST(rohe_scheidungsziffer as DECIMAL(5,2)),
		birth_rate_per_1000				= CAST(rohe_geburtenziffer as DECIMAL(5,2)),
		death_rate_per_1000				= CAST(rohe_sterbeziffer as DECIMAL(5,2)),
		private_household_count			= anzahl_privathaushalte,
		private_household_size_average 	= CAST("durchschnittliche_haushaltsgrösse_in_personen"   as DECIMAL(5,2)),
		municipality_area_sqkm			= CAST("gesamtfläche_in_km²_1)"   as DECIMAL(5,2)),
		settlement_area_percent			= CAST("siedlungsfläche_in_%%" as DECIMAL(5,2)),
		argricultural_area_percent		= CAST("landwirtschafts-fläche_in_%%" as DECIMAL(5,2)),
		forest_woods_area_percent		= CAST("wald_und_gehölze_in_%%" as DECIMAL(5,2)),
		unproductive_area_percent		= CAST("unproduktive_fläche_in_%%" as DECIMAL(5,2)),
		employed_total					= CAST("beschäftigte_total" as INTEGER),
		employed_first_sector_count		= CAST("im_1._sektor" as INTEGER),
		employed_second__sector_count	= CAST("im_2._sektor" as INTEGER),
		employed_third_sector_count		= CAST("im_3._sektor" as INTEGER),
		workplaces_total				= CAST("arbeitsstätten_total" as INTEGER),
		workplaces_first_sector_count	= CAST("im_1._sektor.1" as INTEGER),
		workplaces_second_sector_count	= CAST("im_2._sektor.1" as INTEGER),
		workplaces_third_sector_count	= CAST("im_3._sektor.1" as INTEGER),
		vacancy_rate					= CAST("leerwohnungs-ziffer" as DECIMAL(5,2)),
		new_build_apartment_per_1000_population	=  CAST(neu_gebaute_wohnungen_pro_1000_einwohner   as DECIMAL(5,2)),
		social_assistance_rate			= CAST(sozialhilfequote as DECIMAL(5,2)),
		political_party_fdp_percent		= CAST("fdp_2)" as DECIMAL(5,2)),
		political_party_cvp_percent		= CAST(cvp  as DECIMAL(5,2)),
		political_party_sp_percent		= CAST(sp as DECIMAL(5,2)),
		political_party_svp_percent		= CAST(svp  as DECIMAL(5,2)),
		political_party_evp_percent		= CAST("evp/csp"  as DECIMAL(5,2)),
		political_party_glp_percent		= CAST(glp  as DECIMAL(5,2)),
		political_party_bdp_percent		= CAST(bdp  as DECIMAL(5,2)),
		political_party_pda_sol_percent	= CAST("pda/sol."  as DECIMAL(5,2)),
		political_party_gps_percent		= CAST(gps  as DECIMAL(5,2)),
		political_party_small_right_wing_percent	= CAST(kleine_rechtsparteien as DECIMAL(5,2)),
    	dwh_change_date 				= now(),
    	dwh_status 						= 'A'

FROM 	tmp_municipality_statistic B
WHERE 	CAST(gemeindecode as INTEGER) = A.municipality_id
AND ( 	population_count 					!= CAST(einwohner as INTEGER)
		OR population_density_sqkm			!= CAST("bevölkerungs-dichte_pro_km²" as INTEGER)
		OR population_foreigner_percent		!= CAST("ausländer_in_%%" as DECIMAL(5,2))
		OR age_0_19_percent					!= CAST("0-19_jahre" as DECIMAL(5,2))
		OR age_20_64_percent				!= CAST("20-64_jahre" as DECIMAL(5,2))
		OR age_65_plus_percent				!= CAST("65_jahre_und_mehr" as DECIMAL(5,2))
		OR marriage_rate_per_1000			!= CAST(rohe_heiratssziffer as DECIMAL(5,2))
		OR divorce_rate_per_1000			!= CAST(rohe_scheidungsziffer as DECIMAL(5,2))
		OR birth_rate_per_1000				!= CAST(rohe_geburtenziffer as DECIMAL(5,2))
		OR death_rate_per_1000				!= CAST(rohe_sterbeziffer as DECIMAL(5,2))
		OR private_household_count			!= anzahl_privathaushalte
		OR private_household_size_average 	!= CAST("durchschnittliche_haushaltsgrösse_in_personen"   as DECIMAL(5,2))
		OR municipality_area_sqkm			!= CAST("gesamtfläche_in_km²_1)"   as DECIMAL(5,2))
		OR settlement_area_percent			!= CAST("siedlungsfläche_in_%%" as DECIMAL(5,2))
		OR argricultural_area_percent		!= CAST("landwirtschafts-fläche_in_%%" as DECIMAL(5,2))
		OR forest_woods_area_percent		!= CAST("wald_und_gehölze_in_%%" as DECIMAL(5,2))
		OR unproductive_area_percent		!= CAST("unproduktive_fläche_in_%%" as DECIMAL(5,2))
		OR employed_total					!= CAST("beschäftigte_total" as INTEGER)
		OR employed_first_sector_count		!= CAST("im_1._sektor" as INTEGER)
		OR employed_second__sector_count	!= CAST("im_2._sektor" as INTEGER)
		OR employed_third_sector_count		!= CAST("im_3._sektor" as INTEGER)
		OR workplaces_total					!= CAST("arbeitsstätten_total" as INTEGER)
		OR workplaces_first_sector_count	!= CAST("im_1._sektor.1" as INTEGER)
		OR workplaces_second_sector_count	!= CAST("im_2._sektor.1" as INTEGER)
		OR workplaces_third_sector_count	!= CAST("im_3._sektor.1" as INTEGER)
		OR vacancy_rate						!= CAST("leerwohnungs-ziffer" as DECIMAL(5,2))
		OR new_build_apartment_per_1000_population	!=  CAST(neu_gebaute_wohnungen_pro_1000_einwohner   as DECIMAL(5,2))
		OR social_assistance_rate			!= CAST(sozialhilfequote as DECIMAL(5,2))
		OR political_party_fdp_percent		!= CAST("fdp_2)" as DECIMAL(5,2))
		OR political_party_cvp_percent		!= CAST(cvp  as DECIMAL(5,2))
		OR political_party_sp_percent		!= CAST(sp as DECIMAL(5,2))
		OR political_party_svp_percent		!= CAST(svp  as DECIMAL(5,2))
		OR political_party_evp_percent		!= CAST("evp/csp"  as DECIMAL(5,2))
		OR political_party_glp_percent		!= CAST(glp  as DECIMAL(5,2))
		OR political_party_bdp_percent		!= CAST(bdp  as DECIMAL(5,2))
		OR political_party_pda_sol_percent	!= CAST("pda/sol."  as DECIMAL(5,2))
		OR political_party_gps_percent		!= CAST(gps  as DECIMAL(5,2))
		OR political_party_small_right_wing_percent	!= CAST(kleine_rechtsparteien as DECIMAL(5,2))
        OR A.dwh_status 					!= 'A'
          )
;
-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Update step done');

-- *** INSERT *** ---
INSERT INTO	dwh.t_municipality_statistic
SELECT 	CAST(gemeindecode as INTEGER),
		CAST(einwohner as INTEGER),
		CAST("bevölkerungs-dichte_pro_km²" as INTEGER),
		CAST("ausländer_in_%%" as DECIMAL(5,2)),
		CAST("0-19_jahre" as DECIMAL(5,2)),
		CAST("20-64_jahre" as DECIMAL(5,2)),
		CAST("65_jahre_und_mehr" as DECIMAL(5,2)),
		CAST(rohe_heiratssziffer as DECIMAL(5,2)),
		CAST(rohe_scheidungsziffer as DECIMAL(5,2)),
		CAST(rohe_geburtenziffer as DECIMAL(5,2)),
		CAST(rohe_sterbeziffer as DECIMAL(5,2)),
		anzahl_privathaushalte,
		CAST("durchschnittliche_haushaltsgrösse_in_personen"   as DECIMAL(5,2)),
		CAST("gesamtfläche_in_km²_1)"   as DECIMAL(5,2)),
		CAST("siedlungsfläche_in_%%" as DECIMAL(5,2)),
		CAST("landwirtschafts-fläche_in_%%" as DECIMAL(5,2)),
		CAST("wald_und_gehölze_in_%%" as DECIMAL(5,2)),
		CAST("unproduktive_fläche_in_%%" as DECIMAL(5,2)),
		CAST("beschäftigte_total" as INTEGER),
		CAST("im_1._sektor" as INTEGER),
		CAST("im_2._sektor" as INTEGER),
		CAST("im_3._sektor" as INTEGER),
		CAST("arbeitsstätten_total" as INTEGER),
		CAST("im_1._sektor.1" as INTEGER),
		CAST("im_2._sektor.1" as INTEGER),
		CAST("im_3._sektor.1" as INTEGER),
		CAST("leerwohnungs-ziffer" as DECIMAL(5,2)),
		CAST(neu_gebaute_wohnungen_pro_1000_einwohner   as DECIMAL(5,2)),
		CAST(sozialhilfequote as DECIMAL(5,2)),
		CAST("fdp_2)" as DECIMAL(5,2)),
		CAST(cvp  as DECIMAL(5,2)),
		CAST(sp as DECIMAL(5,2)),
		CAST(svp  as DECIMAL(5,2)),
		CAST("evp/csp"  as DECIMAL(5,2)),
		CAST(glp  as DECIMAL(5,2)),
		CAST(bdp  as DECIMAL(5,2)),
		CAST("pda/sol."  as DECIMAL(5,2)),
		CAST(gps  as DECIMAL(5,2)),
		CAST(kleine_rechtsparteien as DECIMAL(5,2))
FROM 	tmp_municipality_statistic A
WHERE NOT EXISTS (SELECT 	1
				  FROM 		dwh.t_municipality_statistic B
                  WHERE 	B.municipality_id = CAST(gemeindecode as INTEGER))
;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Insert step done');

-- Drop temp table
DROP TABLE tmp_municipality_statistic;

-- Log
CALL job.p_log (job_level, ip_job_id, 0, 'Job end');

END;
$BODY$;

ALTER PROCEDURE job.p_load_dwh_1(integer)
    OWNER TO postgres;



