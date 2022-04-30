CREATE TABLE IF NOT EXISTS t_municipality_statistic
(
statistic_id SERIAL PRIMARY KEY,
municipality_id INT NOT NULL,
population_count INT,
population_changed_last_year DECIMAL(5,4),
population_density_sqkm INT,
population_foreigner_percent DECIMAL(5,4),
age_0_19_percent DECIMAL(5,4),
age_20_64_percent DECIMAL(5,4),
age_65_plus_percent DECIMAL(5,4),
marriage_rate_per_1000 SMALLINT,
divorce_number_per_1000 SMALLINT,
birth_rate  DECIMAL(5,4),
death_rate  DECIMAL(5,4),
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
political_party_small_right_wing_percent DECIMAL(5,4)
)

CREATE TABLE IF NOT EXISTS dwh.t_canton
(
canton_id VARCHAR(2) PRIMARY KEY,
canton_name
)

CREATE TABLE IF NOT EXISTS dwh.t_sbb_statistic
(
statistic_id SERIAL PRIMARY KEY,
municipality_id INT NOT NULL,
stops_count SMALLINT,
stop_density_sqkm SMALLINT

)

CREATE TABLE IF NOT EXISTS dwh.t_weather_statistic
(
statistic_id SERIAL PRIMARY KEY,
municipality_id INT NOT NULL,
radiation_daily_avg_wsqm SMALLINT,
snow_cm SMALLINT,
cloudyness_percent DECIMAL(5,4),
air_pressure_hpa DECIMAL(5,1),
rain_mm SMALLINT,
sunshine_hours SMALLINT,
air_temp_avg_celsius SMALLINT,
air_temp_min_celsius SMALLINT,
air_temp_max_celsius SMALLINT,
air_temp_percent_celsius SMALLINT,
humidity_avg_percent DECIMAL(5,4)
)


CREATE TABLE IF NOT EXISTS dwh.t_municipality
(
municipality_id INT PRIMARY KEY,
plz_id INT,
municipality_name VARCHAR(200),
CONSTRAINT fk_canton_id
    FOREIGN KEY(canton_id)
        REFERENCES t_canton(canton_id)
)
