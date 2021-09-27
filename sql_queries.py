import configparser

# Read config file
config = configparser.ConfigParser()
config.read('aws/dl.cfg')
ARN = config.get('IAM_ROLE', 'ARN')

# Drop existing tables
dim_docking_stations_drop = 'DROP TABLE IF EXISTS dim_docking_stations;'
dim_time_table_drop = 'DROP TABLE IF EXISTS dim_time;'
dim_journey_distances_drop = 'DROP TABLE IF EXISTS dim_journey_distances;'
dim_daily_weather_table_drop = 'DROP TABLE IF EXISTS dim_daily_weather;'
fact_journeys_table_drop = 'DROP TABLE IF EXISTS fact_journeys;'

# Create tables

dim_docking_stations_create = ("""
    CREATE TABLE IF NOT EXISTS dim_docking_stations (
     docking_station_live_date VARCHAR(255),
     docking_station_name VARCHAR(255),
     docking_station_id INTEGER,
     docking_points INTEGER,
     docking_station_latitude DOUBLE PRECISION,
     docking_station_longitude DOUBLE PRECISION
    )diststyle all;
""")

dim_time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_time (
    rental_start_date TIMESTAMP NOT NULL,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL
    )diststyle all;
""")

dim_journey_distances_create = ("""
    CREATE TABLE IF NOT EXISTS dim_journey_distances (
    rental_id INTEGER NOT NULL,
    start_station_id INTEGER,
    start_lat DOUBLE PRECISION,
    start_lon DOUBLE PRECISION,
    rental_start_year INTEGER,
    rental_start_month INTEGER,
    rental_start_day INTEGER,
    end_station_id INTEGER,
    end_lat DOUBLE PRECISION,
    end_lon DOUBLE PRECISION,
    journey_distance_km DOUBLE PRECISION
    )diststyle even;
""")

dim_daily_weather_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_daily_weather (
    date TIMESTAMP NOT NULL,
    year INTEGER,
    month INTEGER,
    day_of_month INTEGER,
    conditions VARCHAR(255),
    description VARCHAR(255),
    avg_temp DOUBLE PRECISION,
    min_temp DOUBLE PRECISION,
    max_temp DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    windspeed DOUBLE PRECISION,
    sunrise VARCHAR(255),
    sunset VARCHAR(255)
    )diststyle all;
""")

fact_journeys_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_journeys (
    rental_id INTEGER NOT NULL,
    bike_id INTEGER NOT NULL,
    rental_duration_seconds INTEGER,
    start_station_id INTEGER,
    rental_start_date TIMESTAMP,
    end_station_id INTEGER,
    rental_end_date TIMESTAMP,
    rental_start_year INTEGER,
    rental_start_month INTEGER,
    rental_start_day INTEGER
    )diststyle even;
""")

# Copy data queries

dim_docking_stations_copy = ("""
    COPY dim_docking_stations
    FROM 's3://lnd-bikehire/infrastructure/docking_stations/'
    iam_role {}
    format as parquet;
""").format(ARN)

dim_time_table_copy = ("""
    COPY dim_time
    FROM 's3://lnd-bikehire/time/'
    iam_role {}
    format as parquet;
""").format(ARN)

dim_journey_distances_copy = ("""
    COPY dim_journey_distances
    FROM 's3://lnd-bikehire/journey_distances/'
    iam_role {}
    format as parquet;
""").format(ARN)

dim_daily_weather_table_copy = ("""
    COPY dim_daily_weather
    FROM 's3://lnd-bikehire/weather/'
    iam_role {}
    format as parquet;
""").format(ARN)

fact_journeys_table_copy = ("""
    COPY fact_journeys
    FROM 's3://lnd-bikehire/journeys/'
    iam_role {}
    format as parquet;
""").format(ARN)

# Query lists
drop_table_queries = [dim_docking_stations_drop, dim_time_table_drop, dim_journey_distances_drop,
                      dim_daily_weather_table_drop, fact_journeys_table_drop]
create_table_queries = [dim_docking_stations_create, dim_time_table_create, dim_journey_distances_create,
                        dim_daily_weather_table_create, fact_journeys_table_create]
copy_table_queries = [dim_docking_stations_copy, dim_time_table_copy, dim_journey_distances_copy,
                      dim_daily_weather_table_copy, fact_journeys_table_copy]
