# push_to_postgres_python_script.py
import psycopg2
import pandas as pd
from io import StringIO

DB_NAME = "air_pollution_dw"
DB_USER = "postgres"
DB_PASSWORD = "1234"
DB_HOST = "localhost"
DB_PORT = "5432"

def ensure_staging_tables_exist():
    """
    Ensure the staging schema and tables exist before loading data.
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    # Execute each statement separately
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.pm25_air_pollution_raw (
            country_name    VARCHAR(200),
            country_code    VARCHAR(3),
            year            INT,
            pm25_air_pollution_ug_m3 NUMERIC
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.death_rate_air_pollution_raw (
            country_area    VARCHAR(200),
            year            INT,
            death_rate_per_100k NUMERIC
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.urban_population_raw (
            country_name    VARCHAR(200),
            country_code    VARCHAR(3),
            year            INT,
            urban_population_pct NUMERIC
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.industrial_gdp_raw (
            country_name    VARCHAR(200),
            country_code    VARCHAR(3),
            year            INT,
            industrial_gdp_pct NUMERIC
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.population_density_raw (
            entity          VARCHAR(200),
            code            VARCHAR(20),
            year            INT,
            population_density NUMERIC
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Staging schema and tables verified/created.")

def copy_from_dataframe(df, table_name, conn):
    """
    Efficiently load a pandas DataFrame into Postgres using COPY.
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cur = conn.cursor()
    # Use copy_expert for better schema support
    copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '')"
    cur.copy_expert(copy_sql, buffer)
    conn.commit()
    cur.close()

def load_staging_tables():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    
    # Ensure staging tables exist in this connection
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    
    # Drop and recreate staging tables to ensure correct schema
    cur.execute("DROP TABLE IF EXISTS staging.population_density_raw CASCADE;")
    cur.execute("DROP TABLE IF EXISTS staging.industrial_gdp_raw CASCADE;")
    cur.execute("DROP TABLE IF EXISTS staging.urban_population_raw CASCADE;")
    cur.execute("DROP TABLE IF EXISTS staging.death_rate_air_pollution_raw CASCADE;")
    cur.execute("DROP TABLE IF EXISTS staging.pm25_air_pollution_raw CASCADE;")
    
    cur.execute("""
        CREATE TABLE staging.pm25_air_pollution_raw (
            country_name    VARCHAR(200),
            country_code    VARCHAR(3),
            year            INT,
            pm25_air_pollution_ug_m3 NUMERIC
        );
    """)
    
    cur.execute("""
        CREATE TABLE staging.death_rate_air_pollution_raw (
            country_area    VARCHAR(200),
            year            INT,
            death_rate_per_100k NUMERIC
        );
    """)
    
    cur.execute("""
        CREATE TABLE staging.urban_population_raw (
            country_name    VARCHAR(200),
            country_code    VARCHAR(3),
            year            INT,
            urban_population_pct NUMERIC
        );
    """)
    
    cur.execute("""
        CREATE TABLE staging.industrial_gdp_raw (
            country_name    VARCHAR(200),
            country_code    VARCHAR(3),
            year            INT,
            industrial_gdp_pct NUMERIC
        );
    """)
    
    cur.execute("""
        CREATE TABLE staging.population_density_raw (
            entity          VARCHAR(200),
            code            VARCHAR(20),
            year            INT,
            population_density NUMERIC
        );
    """)
    
    conn.commit()
    cur.close()
    print("Staging schema and tables verified/created.")

    # 1) pm25-air-pollution.csv
    pm25 = pd.read_csv("pm25-air-pollution.csv")
    pm25 = pm25.rename(columns={
        "Country Name": "country_name",
        "Country Code": "country_code",
        "Year": "year",
        "PM2.5 air pollution µg/m³": "pm25_air_pollution_ug_m3"
    })
    copy_from_dataframe(pm25, "staging.pm25_air_pollution_raw", conn)

    # 2) Death rate
    death = pd.read_csv("Death rate from air pollution per 100K people 1990 - 2022.csv")
    death = death.rename(columns={
        "Country/area": "country_area",
        "Year": "year",
        "Death rate from air pollution per 100K people": "death_rate_per_100k"
    })
    copy_from_dataframe(death, "staging.death_rate_air_pollution_raw", conn)

    # 3) Urban Pop
    urban = pd.read_csv("Urban Pop..csv")
    urban = urban.rename(columns={
        "Country Name": "country_name",
        "Country Code": "country_code",
        "Year": "year",
        "Urban population %": "urban_population_pct"
    })
    copy_from_dataframe(urban, "staging.urban_population_raw", conn)

    # 4) Industrial GDP
    ind = pd.read_csv("Industrial GDP %.csv")
    ind = ind.rename(columns={
        "Country Name": "country_name",
        "Country Code": "country_code",
        "Year": "year",
        "Industrial GDP %": "industrial_gdp_pct"
    })
    copy_from_dataframe(ind, "staging.industrial_gdp_raw", conn)

    # 5) Population density
    dens = pd.read_csv("population-density.csv")
    dens = dens.rename(columns={
        "Entity": "entity",
        "Code": "code",
        "Year": "year",
        "Population density": "population_density"
    })
    copy_from_dataframe(dens, "staging.population_density_raw", conn)

    conn.close()
    print("Staging tables loaded.")

def build_dim_location():
    """
    Build dim_location from pm25 canonical list.
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO dim_location (country_code, country_name)
        SELECT DISTINCT country_code, country_name
        FROM staging.pm25_air_pollution_raw
        WHERE country_code IS NOT NULL
        ON CONFLICT (country_code) DO NOTHING;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("dim_location built from pm25 data.")

def build_fact_table():
    """
    Join all staging tables on (country_code, year) where possible.
    Death rate table joins on country name, so we map by country_name.
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    # Clear fact table if reloading
    cur.execute("DELETE FROM fact_air_pollution_burden;")

    insert_sql = """
        INSERT INTO fact_air_pollution_burden (
            location_key,
            date_key,
            pm25_air_pollution_ug_m3,
            death_rate_per_100k,
            industrial_gdp_pct,
            urban_population_pct,
            population_density
        )
        SELECT
            dl.location_key,
            dd.date_key,
            pm.pm25_air_pollution_ug_m3,
            dr.death_rate_per_100k,
            ig.industrial_gdp_pct,
            up.urban_population_pct,
            pd.population_density
        FROM staging.pm25_air_pollution_raw pm
        JOIN dim_location dl
            ON dl.country_code = pm.country_code
        JOIN dim_date dd
            ON dd.year = pm.year
        LEFT JOIN staging.industrial_gdp_raw ig
            ON ig.country_code = pm.country_code
           AND ig.year = pm.year
        LEFT JOIN staging.urban_population_raw up
            ON up.country_code = pm.country_code
           AND up.year = pm.year
        LEFT JOIN staging.population_density_raw pd
            ON pd.code = pm.country_code
           AND pd.year = pm.year
        LEFT JOIN staging.death_rate_air_pollution_raw dr
            ON dr.country_area = pm.country_name
           AND dr.year = pm.year;
    """

    cur.execute(insert_sql)
    conn.commit()
    cur.close()
    conn.close()
    print("fact_air_pollution_burden populated.")

if __name__ == "__main__":
    load_staging_tables()
    build_dim_location()
    build_fact_table()
