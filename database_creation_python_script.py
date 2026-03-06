# database_creation_python_script.py
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DB_NAME = "air_pollution_dw"
DB_USER = "postgres"
DB_PASSWORD = "1234"
DB_HOST = "localhost"
DB_PORT = "5432"

def create_database():
    conn = psycopg2.connect(
        dbname="postgres",
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
    if cur.fetchone() is None:
        cur.execute(f'CREATE DATABASE "{DB_NAME}";')
        print(f"Database {DB_NAME} created.")
    else:
        print(f"Database {DB_NAME} already exists.")

    cur.close()
    conn.close()

def create_dw_schema_and_tables():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    ddl_statements = """

    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS dim_date (
        date_key        SERIAL PRIMARY KEY,
        year            INT UNIQUE NOT NULL,
        decade          INT,
        period_label    VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS dim_location (
        location_key        SERIAL PRIMARY KEY,
        country_code        VARCHAR(3) UNIQUE NOT NULL,
        country_name        VARCHAR(200) NOT NULL,
        region              VARCHAR(100),
        income_group        VARCHAR(50),
        development_status  VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS fact_air_pollution_burden (
        fact_id                 BIGSERIAL PRIMARY KEY,
        location_key            INT NOT NULL REFERENCES dim_location(location_key),
        date_key                INT NOT NULL REFERENCES dim_date(date_key),

        pm25_air_pollution_ug_m3        NUMERIC,
        death_rate_per_100k             NUMERIC,
        industrial_gdp_pct              NUMERIC,
        urban_population_pct            NUMERIC,
        population_density              NUMERIC
    );

    CREATE TABLE IF NOT EXISTS staging.pm25_air_pollution_raw (
        country_name    VARCHAR(200),
        country_code    VARCHAR(3),
        year            INT,
        pm25_air_pollution_ug_m3 NUMERIC
    );

    CREATE TABLE IF NOT EXISTS staging.death_rate_air_pollution_raw (
        country_area    VARCHAR(200),
        year            INT,
        death_rate_per_100k NUMERIC
    );

    CREATE TABLE IF NOT EXISTS staging.urban_population_raw (
        country_name    VARCHAR(200),
        country_code    VARCHAR(3),
        year            INT,
        urban_population_pct NUMERIC
    );

    CREATE TABLE IF NOT EXISTS staging.industrial_gdp_raw (
        country_name    VARCHAR(200),
        country_code    VARCHAR(3),
        year            INT,
        industrial_gdp_pct NUMERIC
    );

    CREATE TABLE IF NOT EXISTS staging.population_density_raw (
        entity          VARCHAR(200),
        code            VARCHAR(3),
        year            INT,
        population_density NUMERIC
    );

    """

    cur.execute(ddl_statements)
    conn.commit()
    cur.close()
    conn.close()
    print("Schemas and tables created.")

def populate_dim_date(start_year=1990, end_year=2030):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    for year in range(start_year, end_year + 1):
        decade = (year // 10) * 10
        period_label = f"{decade}s"
        cur.execute(
            """
            INSERT INTO dim_date (year, decade, period_label)
            VALUES (%s, %s, %s)
            ON CONFLICT (year) DO NOTHING;
            """,
            (year, decade, period_label)
        )

    conn.commit()
    cur.close()
    conn.close()
    print("dim_date populated.")

if __name__ == "__main__":
    create_database()
    create_dw_schema_and_tables()
    populate_dim_date(1990, 2025)
