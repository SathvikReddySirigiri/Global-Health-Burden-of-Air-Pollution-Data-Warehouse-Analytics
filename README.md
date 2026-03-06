#Global Health Burden of Air Pollution – Data Warehouse & Analytics
##Project Overview
Air pollution is one of the most significant environmental and public health challenges worldwide. Fine particulate matter (PM2.5) is particularly dangerous because it can penetrate deep into the lungs and bloodstream, increasing the risk of respiratory and cardiovascular diseases.

This project analyzes the global health burden of air pollution by integrating multiple datasets related to PM2.5 exposure, air‑pollution‑related mortality, and socioeconomic indicators.

The project implements a data warehouse architecture with an ETL pipeline to integrate global datasets and enable analytical queries and visualization of pollution trends and health impacts.

##Objectives
•	Analyze global PM2.5 air pollution trends
•	Understand the relationship between PM2.5 exposure and mortality rates
•	Compare pollution exposure and mortality across regions and countries
•	Examine the influence of industrialization, urbanization, and population density
•	Build a data warehouse to support analytical queries and dashboards

##Technologies Used
•	Python
•	Pandas
•	PostgreSQL
•	Psycopg2
•	SQL
•	Tableau
•	Dimensional Modeling
•	ETL Pipeline

##Data Sources
The project integrates five main datasets at the country–year level:
1. PM2.5 air pollution levels (µg/m³)
2. Death rate from air pollution per 100k people
3. Urban population percentage
4. Industrial GDP percentage
5. Population density

These datasets were obtained from global public data sources including WHO and other international open data repositories.
Data Warehouse Architecture
The project follows a classic ETL‑based data warehouse architecture with the following layers:

Source Layer – Raw CSV datasets containing pollution, health, and socioeconomic data.
Staging Layer – Raw datasets loaded into PostgreSQL staging tables.
Warehouse Layer – Cleaned and integrated analytical tables using dimensional modeling.
Visualization Layer – Tableau dashboards used for analysis and storytelling.
Dimensional Model (Star Schema)
Fact Table
fact_air_pollution_burden

Measures include:
•	PM2.5 concentration
•	Death rate per 100k people
•	Industrial GDP percentage
•	Urban population percentage
•	Population density
Grain: One record per country per year
Dimension Tables
dim_date
- date_key
- year
- decade
- period_label

dim_location
- location_key
- country_code
- country_name
- region
- income_group
- development_status
ETL Pipeline
Extract
CSV files are read into Python using Pandas dataframes.
Transform
Data is cleaned and standardized, column names are normalized, datasets are aligned by country and year, and missing values are handled. Dimension tables are also generated during transformation.
Load
Cleaned data is loaded into PostgreSQL staging tables and then integrated into fact and dimension tables using SQL joins.
Dashboard & Visualizations
Tableau dashboards were created to explore global pollution patterns, mortality trends, and the influence of socioeconomic factors. Key visualizations include:
- Global PM2.5 concentration map
- PM2.5 vs mortality scatter plot
- Regional pollution and mortality comparison
- Industrialization vs pollution trends
Key Insights
•	Higher PM2.5 exposure strongly correlates with increased mortality rates.
•	Africa and Asia experience the highest pollution levels and health burden.
•	Global PM2.5 levels and pollution‑related mortality have decreased over time but still exceed safe thresholds in many regions.
•	Industrial activity and population density significantly influence pollution exposure.
Future Improvements
•	Integrate real‑time air quality datasets
•	Add country‑level economic and policy indicators
•	Develop automated data pipelines
•	Expand dashboards with predictive analytics
Team
Naga Pavan Sathvik Reddy Sirigiri
Aravind Polavarapu
Mohammad Sahil

