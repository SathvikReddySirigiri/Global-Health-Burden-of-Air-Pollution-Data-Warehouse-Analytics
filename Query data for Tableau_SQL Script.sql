-- Query data for Tableau_SQL Script.sql

-- =========================
-- Helper View: Fact + Dims
-- =========================
CREATE OR REPLACE VIEW vw_air_pollution_burden AS
SELECT
    f.fact_id,
    d.year,
    loc.country_name,
    loc.country_code,
    loc.region,
    loc.income_group,
    loc.development_status,
    f.pm25_air_pollution_ug_m3,
    f.death_rate_per_100k,
    f.industrial_gdp_pct,
    f.urban_population_pct,
    f.population_density
FROM fact_air_pollution_burden f
JOIN dim_date d
    ON f.date_key = d.date_key
JOIN dim_location loc
    ON f.location_key = loc.location_key;

-- =====================================
-- Q1: Evolution of global PM2.5 levels
-- =====================================
-- Global average PM2.5 over time
CREATE OR REPLACE VIEW vw_q1_global_pm25_trend AS
SELECT
    year,
    AVG(pm25_air_pollution_ug_m3) AS avg_pm25_ug_m3
FROM vw_air_pollution_burden
GROUP BY year
ORDER BY year;

-- ===========================================
-- Q2: Relationship PM2.5 vs Mortality
-- ===========================================
-- Country-year scatter data
CREATE OR REPLACE VIEW vw_q2_pm25_vs_deathrate AS
SELECT
    country_name,
    country_code,
    year,
    pm25_air_pollution_ug_m3,
    death_rate_per_100k,
    industrial_gdp_pct,
    urban_population_pct,
    population_density,
    region,
    income_group,
    development_status
FROM vw_air_pollution_burden
WHERE pm25_air_pollution_ug_m3 IS NOT NULL
  AND death_rate_per_100k IS NOT NULL;

-- Example analytic query: average death rate by PM2.5 buckets
CREATE OR REPLACE VIEW vw_q2_pm25_bucket_summary AS
WITH binned AS (
    SELECT
        *,
        WIDTH_BUCKET(pm25_air_pollution_ug_m3, 0, 100, 5) AS pm25_bucket
    FROM vw_air_pollution_burden
    WHERE pm25_air_pollution_ug_m3 IS NOT NULL
      AND death_rate_per_100k IS NOT NULL
)
SELECT
    pm25_bucket,
    MIN(pm25_air_pollution_ug_m3) AS min_pm25,
    MAX(pm25_air_pollution_ug_m3) AS max_pm25,
    AVG(death_rate_per_100k) AS avg_death_rate_per_100k
FROM binned
GROUP BY pm25_bucket
ORDER BY pm25_bucket;

-- ======================================================
-- Q3: Regions/Countries with highest death rates
-- ======================================================

-- Top countries by death rate in a given year (param: :year_param)
-- (Use this as a template in Tableau with a year filter)
CREATE OR REPLACE VIEW vw_q3_top_countries_deathrate AS
SELECT
    year,
    country_name,
    country_code,
    region,
    death_rate_per_100k
FROM vw_air_pollution_burden
WHERE death_rate_per_100k IS NOT NULL;

-- Region-level averages
CREATE OR REPLACE VIEW vw_q3_region_deathrate AS
SELECT
    year,
    region,
    AVG(death_rate_per_100k) AS avg_death_rate_per_100k
FROM vw_air_pollution_burden
WHERE region IS NOT NULL
GROUP BY year, region
ORDER BY year, region;

-- =============================================================
-- Q4: Impact of urbanization, industrialization, population density
-- =============================================================

CREATE OR REPLACE VIEW vw_q4_drivers_summary AS
SELECT
    year,
    region,
    income_group,
    AVG(pm25_air_pollution_ug_m3) AS avg_pm25,
    AVG(death_rate_per_100k) AS avg_death_rate,
    AVG(industrial_gdp_pct) AS avg_industrial_gdp_pct,
    AVG(urban_population_pct) AS avg_urban_pop_pct,
    AVG(population_density) AS avg_population_density
FROM vw_air_pollution_burden
GROUP BY year, region, income_group
ORDER BY year, region, income_group;

-- You can use this in Tableau to:
-- * Plot avg_pm25 vs avg_urban_pop_pct
-- * Plot avg_pm25 vs avg_industrial_gdp_pct
-- * Color/size by avg_death_rate

-- =============================================================
-- Q5: Differences by income group / development status
-- =============================================================

-- Income group comparison
CREATE OR REPLACE VIEW vw_q5_income_group_comparison AS
SELECT
    year,
    income_group,
    AVG(pm25_air_pollution_ug_m3) AS avg_pm25,
    AVG(death_rate_per_100k) AS avg_death_rate,
    AVG(urban_population_pct) AS avg_urban_pop_pct,
    AVG(industrial_gdp_pct) AS avg_industrial_gdp_pct,
    AVG(population_density) AS avg_population_density
FROM vw_air_pollution_burden
WHERE income_group IS NOT NULL
GROUP BY year, income_group
ORDER BY year, income_group;

-- Development status comparison
CREATE OR REPLACE VIEW vw_q5_development_status_comparison AS
SELECT
    year,
    development_status,
    AVG(pm25_air_pollution_ug_m3) AS avg_pm25,
    AVG(death_rate_per_100k) AS avg_death_rate
FROM vw_air_pollution_burden
WHERE development_status IS NOT NULL
GROUP BY year, development_status
ORDER BY year, development_status;
