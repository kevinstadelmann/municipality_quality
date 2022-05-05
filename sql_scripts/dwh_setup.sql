-- *****************************
-- Table t_canton
-- *****************************
CREATE TABLE IF NOT EXISTS dwh.t_canton
(
canton_id SMALLINT PRIMARY KEY,
canton_name_short VARCHAR (2)
canton_name VARCHAR(50)
)

-- *** INSERT *** ---
INSERT INTO dwh.t_canton
SELECT	distinct SBB.kantonsnum, gdekt, gdektna
FROM	stage.t_municipality_district	MUN

INNER	JOIN stage.t_sbb_stop			SBB
ON		SBB.kantonskuerzel = MUN.gdekt

-- *****************************
-- Table t_municipality
-- *****************************
CREATE TABLE dwh.t_municipality
(
municipality_id INT PRIMARY KEY,
name VARCHAR(100) NOT NULL,
district_id INT,
canton_id INT NOT NULL,
e_cntr INT NOT NULL,
n_cntr INT NOT NULL,
CONSTRAINT fk_canton_id
    FOREIGN KEY(canton_id)
        REFERENCES dwh.t_canton(canton_id)
)

-- *** INSERT *** ---
INSERT INTO dwh.t_municipality
SELECT 	gmdnr,
		gmdname,
		bznr,
		ktnr,
		e_cntr,
		n_cntr
FROM 	stage.t_municipality_location

-- *****************************
-- Table t_sbb_stop
-- *****************************
CREATE TABLE IF NOT EXISTS dwh.t_sbb_stop
(
stop_id INT PRIMARY KEY,
municipality_id INT NOT NULL,
stop_name VARCHAR(100),
stop_type VARCHAR(50),
CONSTRAINT fk_municipality_id
    FOREIGN KEY(municipality_id)
        REFERENCES dwh.t_municipality(municipality_id)
)

-- *** INSERT *** ---
INSERT INTO dwh.t_sbb_stop
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
-- *****************************
-- Table t_municipality_statistics
-- *****************************

CREATE TABLE IF NOT EXISTS dwh.t_municipality_statistic
(
statistic_id SERIAL PRIMARY KEY,
municipality_id INT NOT NULL,
population_count INT,
population_density_sqkm INT,
population_foreigner_percent DECIMAL(5,4),
age_0_19_percent DECIMAL(5,4),
age_20_64_percent DECIMAL(5,4),
age_65_plus_percent DECIMAL(5,4),
marriage_rate_per_1000 SMALLINT,
divorce_rate_per_1000 SMALLINT,
birth_rate_per_1000  DECIMAL(5,4),
death_rate_per_1000  DECIMAL(5,4),
private_household_count INT,
private_household_size_average DECIMAL(5,4),
municipality_area_sqkm INT,
settlement_area_percent  DECIMAL(5,4),
argricultural_area_percent DECIMAL(5,4),
forest_woods_area_percent DECIMAL(5,4),
unproductive_area_percent DECIMAL(5,4),
employed_total INT,
employed_first_sector_count INT,
employed_second__sector_count INT,
employed_third_sector_count INT,
workplaces_total INT,
workplaces_first_sector_count INT,
workplaces_second_sector_count INT,
workplaces_third_sector_count INT,
vacancy_rate DECIMAL(5,4),
new_build_apartment_per_1000_population INT,
social_assistance_rate DECIMAL(5,4),
political_party_fdp_percent DECIMAL(5,4),
political_party_cvp_percent DECIMAL(5,4),
political_party_sp_percent DECIMAL(5,4),
political_party_svp_percent DECIMAL(5,4),
political_party_evp_percent DECIMAL(5,4),
political_party_glp_percent DECIMAL(5,4),
political_party_bdp_percent DECIMAL(5,4),
political_party_pda_sol_percent DECIMAL(5,4),
political_party_gps_percent DECIMAL(5,4),
political_party_small_right_wing_percent DECIMAL(5,4),
CONSTRAINT fk_municipality_id
    FOREIGN KEY(municipality_id)
        REFERENCES dwh.t_municipality(municipality_id)
)

-- *** INSERT *** ---

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

-- CAST and INSERT
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
FROM 	tmp_municipality_statistic


-- *****************************
-- Table t_weather_station
-- *****************************

CREATE TABLE IF NOT EXISTS dwh.t_weather_station(
station_id VARCHAR(3) PRIMARY KEY,
name VARCHAR(50),
coordinatese INT,
coordinatesn INT,
latitude double precision,
longitude double precision
)

-- *** INSERT *** ---
INSERT INTO dwh.t_weather_station
SELECT 	"station/location",
		station,
		CAST(coordinatese as INT),
		CAST(coordinatesn as INT),
		latitude,
		longitude
FROM 	stage.t_weather_station
WHERE	station IS NOT NULL


-- *****************************
-- Table dwh.t_weather_statistic
-- *****************************

CREATE TABLE IF NOT EXISTS dwh.t_weather_statistic
(
statistic_id VARCHAR(15),
station_id VARCHAR(3),
date DATE,
radiation_daily_avg_wsqm SMALLINT,
snow_cm SMALLINT,
air_pressure_hpa DECIMAL(5,1),
rain_mm SMALLINT,
sunshine_hours DECIMAL(5,1),
air_temp_avg_celsius SMALLINT,
air_temp_min_celsius SMALLINT,
air_temp_max_celsius SMALLINT,
air_temp_percent_celsius SMALLINT,
humidity_avg_percent DECIMAL(5,4),
CONSTRAINT fk_station_id
	FOREIGN KEY(station_id)
        REFERENCES dwh.t_weather_station(station_id)
)

-- *** INSERT *** ---
-- CREATE tmp Table
CREATE TEMPORARY TABLE tmp_weather_statistic AS (
SELECT	*
FROM	stage.t_weather_statistic
) WITH DATA

-- Update misbehaving columns

UPDATE	tmp_weather_statistic
SET		hto000d0 = CASE WHEN hto000d0 = '-' THEN NULL ELSE hto000d0 END,
		rre150d0 = CASE WHEN rre150d0 = '-' THEN NULL ELSE rre150d0 END


-- CAST and INSERT
INSERT INTO dwh.t_weather_statistic
SELECT 	CONCAT("station/location", to_date(date::text, 'YYYYMMDD')),
		"station/location",
		to_date(date::text, 'YYYYMMDD'),
		CAST(gre000d0 as SMALLINT),
		CAST(hto000d0 as SMALLINT),
		CAST(prestad0 as DECIMAL(5,1)),
		CAST(rre150d0 AS DECIMAL(5,1)),
		CAST(sre000d0 as SMALLINT),
		CAST(tre200d0 as SMALLINT),
		CAST(tre200dn as SMALLINT),
		CAST(tre200dx as SMALLINT),
		CAST(ure200d0 as DECIMAL(5,2))
FROM 	tmp_weather_statistic

