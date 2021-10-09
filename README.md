<p align="center">
<img alt="London Bike Hire" src="images/yomex-owo-lndbikehire.jpg" title="London Bike Hire"/>
</p>
Photo by <a href="https://unsplash.com/@yomex4life?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText">Yomex Owo</a> on <a href="https://unsplash.com/collections/3542129/london-city-imges?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText">Unsplash</a>


# Capstone Project: London Public Bicycle Hire
The modelling of Santander Cycles (formerly Barclays Cycle Hire) public bicycle hire journey data, coupled with 
weather data, to gain insight into rental behaviours, and identify any correlation to weather conditions.

## Project Scope
Create an Extract, Transform and Load (ETL) pipeline...

of bicycle hire journey and weather data for the year 2012.
Extracting data from CSV and json source data files, stored within an Amazon S3 data lake, transformed to fact and 
dimension tables, and loaded to a data warehouse for analytical insight.

### Technologies and Architecture
Cloud Data Store = Amazon S3 - Simple Storage Service
Cloud Data Warehouse = AWS Redshift
Data Processing = Amazon EMR - Cluster running Apache Spark

INSERT IMAGE

### Logical Data Model
<p>
<img alt="Logical Data Model" src="images/lndbikehire_logicaldm.png" title="Logical Data Model"/>
</p>
-----
# Dataset
This project comprises data from 3 sources. Data used by this project is stored within an AWS S3 Data lake

### Journey data
Journey details for the year 2012.<br>
18 No. CSV files, amounting to approx. 9 million records for the year.<br>
A single bicycle rental/journey is 1 line of text within the CSV, detailing; the start & end date/time of the bicycle 
hire; the start & end docking station; the rental duration; a unique identifier for the rental; the ID of the bicycle 
used.<br>
Bicycle hire journey data used by this project and all other journey data, to present day, can be accessed via 
https://cycling.data.tfl.gov.uk <br>

Raw journey data is hosted at the following location...

    s3://lnd-bikehire/source_data/journey/

CSV sample...

    Rental Id,Duration,Bike Id,End Date,EndStation Id,EndStation Name,Start Date,StartStation Id,StartStation Name
    9340768,1238,893,04/01/2012 00:20,169,Porchester Place: Paddington,04/01/2012 00:00,224,Whiteley's: Bayswater


### Docking Station data
Details of bicycle docking stations located throughout the city.<br>
A single CSV file with details of 845 docking stations.<br>
A single docking station is 1 line of text within the CSV, detailing; the docking station name; a unique station ID; 
the No. of bicycle docking points; the station geographical coordinates.<br>
This data was acquired through freedom of information request to Transport for London. https://tfl.gov.uk/corporate/transparency/freedom-of-information/foi-request-detail?referenceId=FOI-0689-2122

Raw docking station data is hosted at the following location...

    s3://lnd-bikehire/source_data/infrastructure/

CSV sample...

    Go live,Docking Station,Docking station ID,Docking points,Latitude,Longitude
    Jul-10,"River Street, Clerkenwell",1,19,51.5292,-0.109971

### Weather data
City of London, historic weather data for the year 2012.<br>
Data from an API call was saved as a single json file with 365 days of weather observations<br>
Nested daily weather data detailing numerous observations; Date, temperatures, precipitation, wind speed, sunset and 
sunrise times, textual description of conditions.<br>
This data was acquired with an API call at https://www.visualcrossing.com/weather-api

Raw weather data is hosted at the following location...

    s3://lnd-bikehire/source_data/weather/

JSON sample...

    {
        "queryCost": 366,
        "latitude": 51.5064,
        "longitude": -0.12721,
        "resolvedAddress": "London, England, United Kingdom",
        "address": "London",
        "timezone": "Europe/London",
        "tzoffset": 0.0,
        "days": [
            {
                "datetime": "2012-01-01",
                "datetimeEpoch": 1325376000,
                "tempmax": 13.0,
                "tempmin": 7.5,
                "temp": 11.2,
                "feelslikemax": 13.0,
                "feelslikemin": 4.3,
                "feelslike": 10.4,
                "dew": 9.5,
                "humidity": 89.47,
                "precip": 9.84,
                "precipprob": null,
                "precipcover": 12.5,
                "preciptype": null,
                "snow": null,
                "snowdepth": null,
                "windgust": null,
                "windspeed": 16.4,
                "winddir": 226.0,
                "pressure": 1004.9,
                "cloudcover": 13.2,
                "visibility": 18.2,
                "solarradiation": null,
                "solarenergy": null,
                "uvindex": 0.0,
                "sunrise": "08:06:18",
                "sunriseEpoch": 1325405178,
                "sunset": "16:01:32",
                "sunsetEpoch": 1325433692,
                "moonphase": 0.26,
                "conditions": "Rain",
                "description": "Clear conditions throughout the day with rain.",
                "icon": "rain",
                "stations": [
                    "03769099999",
                    "03672099999",
                    "03781099999",
                    "03772099999",
                    "03770099999"
                ],
                "source": "obs"
            },

-----
# Cloud Data Warehouse Schema
A star schema relational database, with a single fact and multiple dimension tables.<br>
INSERT IMAGE

## Data Dictionary
### Table: dim_daily_weather
Cluster distribution: All<br>

**Column name** | **Data type** | **Column description**
----------- | --------- | ------------------
**date**  | TIMESTAMP | NOT NULL : PRIMARY KEY : Full date of weather (yyyy-mm-dd) 
**year** | INTEGER | Year of weather forecast (yyyy)
**month** | INTEGER | Month of weather forecast (mm)
**day_of_month** | INTEGER | Day of weather forecast (dd)
**conditions** | VARCHAR | Single word descriptor of weather conditions for the day
**description** | VARCHAR | Short description of weather conditions for the day
**avg_temp** | FLOAT | Average air temperature for the day (Celsius)
**min_temp** | FLOAT | Minimum air temperature for the day (Celsius)
**max_temp** | FLOAT | Maximum air temperature for the day (Celsius)
**precipitation** | FLOAT | Rainfall for the day (mm)
**windspeed** | FLOAT | Average windspeed over a minute (mph)
**sunrise** | TIMESTAMP | Sunrise (24hr clock)
**sunset** | TIMESTAMP | Sunset (24hr clock)


### Table: dim_docking_stations
Cluster distribution: All<br>

**Column name** | **Data type** | **Column description**
----------- | --------- | ------------------
**docking_station_live_date**  | VARCHAR | Month-year in service date
**docking_station_name** | VARCHAR | Typically city location: Street + Area
**docking_station_id** | INTEGER | NOT NULL : PRIMARY KEY : Unique station identifier
**docking_points** | INTEGER | No. of docking points for bicycles
**docking_station_latitude** | FLOAT | Geographical coordinate specifying north-south position
**docking_station_longitude** | FLOAT | Geographical coordinate specifying east-west position


### Table: dim_time
Cluster distribution: All<br>

**Column name** | **Data type** | **Column description**
--------------- | ------------- | ----------------------
**rental_start_date**  | TIMESTAMP | NOT NULL : PRIMARY KEY : Full date/time of rental (yyyy-mm-dd hh-mm-ss) 
**hour** | INTEGER | NOT NULL : Hour of rental (hh)
**day** | INTEGER | NOT NULL : Day of month of rental (dd)
**weekday** | INTEGER | NOT NULL : Weekday of rental ie. Monday = 1, Tuesday = 2 .... Sunday = 7 (d)
**week** | INTEGER | NOT NULL : Week of year of rental (ww)
**month** | INTEGER | NOT NULL : Month of year of rental (mm)
**year** | INTEGER | NOT NULL : Year of rental (yyyy)


### Table: dim_journey_distances
Cluster distribution: Even<br>

**Column name** | **Data type** | **Column description**
--------------- | ------------- | ----------------------
**rental_id**  | INTEGER | NOT NULL : PRIMARY KEY : Unique rental identifier 
**start_station_id** | INTEGER | Rental start docking station identifier
**start_lat** | FLOAT | Rental start station geographical coordinate
**start_lon** | FLOAT | Rental start station geographical coordinate
**rental_start_year** | INTEGER | Year of rental (yyyy)
**rental_start_month** | INTEGER | Month of year of rental (mm)
**rental_start_day** | INTEGER | Day of month of rental (dd)
**end_station_id** | INTEGER | Rental end docking station identifier
**end_lat** | FLOAT | Rental end station geographical coordinate
**end_lon** | FLOAT | Rental end station geographical coordinate
**journey_distance_km** | FLOAT | Calculated kilometre distance between start and end stations (#.##)


### Table: fact_journeys
Cluster distribution: Even<br>

**Column name** | **Data type** | **Column description**
--------------- | ------------- | ----------------------
**rental_id**  | INTEGER | NOT NULL : PRIMARY KEY : Unique rental identifier 
**bike_id** | INTEGER | Unique bicycle identifier
**rental_duration_seconds** | INTEGER | Duration of rental in seconds
**start_station_id** | INTEGER | Rental start docking station identifier
**rental_start_date** | TIMESTAMP | Full date/time of rental (yyyy-mm-dd hh-mm-ss)
**end_station_id** | INTEGER | Rental end docking station identifier
**rental_end_date** | TIMESTAMP | Full date/time of rental (yyyy-mm-dd hh-mm-ss)
**rental_start_year** | INTEGER | Year of rental (yyyy)
**rental_start_month** | INTEGER | Month of year of rental (mm)
**rental_start_day** | INTEGER | Day of month of rental (dd)

Sample...

**rental_id** | **bike_id** | **rental_duration_seconds** | **start_station_id** | **rental_start_date** | **end_station_id** | **rental_end_date** | **rental_start_year** | **rental_start_month** | **rental_start_day**
--- | --- | --- | --- | --- | --- | --- | --- | --- | ---
10149896 | 4655	| 515 | 14 | 2012-02-22 07:45:00 | 67 | 2012-02-22 07:54:00 | 2012 | 2 | 22

### Repository

#### Project files and process
- [ **create_tables.py** ] (*Python 3 script*):<br>
  Connects to Redshift cluster, creates database fact and dimension tables as per queries from<br>
  the *sql_queries.py* python file.
  
- [ **sql_queries.py** ] (*Python 3 script*):<br>
  CREATE and COPY SQL statements used by create_tables.py

- [ **dwh_load.py** ] (*Python 3 script*):<br>
  Executes SQL COPY queries on AWS S3 hosted parquet files to populate data warehouse tables<br>
  Runs record count and duplicate record checks on data warehouse tables after data loading.

- [ **etl.py** ] (*Python 3 script*):<br>
  Data processing script; 1) Loads data from S3 hosted CSV and JSON files into Spark staging dataframe. 2) Generates 
  Fact and Dimension dataframes from staging dataframe, after filters, dropped nulls and table schemas are applied.<br>
  3) Writes fact and dimension dataframes back to AWS S3 buckets, as partitioned parquet files.

- [ **dl.cfg** ] (*config text file*):<br>
  Contains user AWS credentials, S3 bucket paths, cluster details, all utilised by project Python scripts.

-----
# Running the project

## Prerequisites
- A running Apache Spark cluster for the data processing. With your Apache Spark deployment of choice!
- AWS Identity and access management (IAM) credentials with permissions for Amazon S3 and Redshift cluster access.
- A running Amazon Redshift cluster for the data warehouse.


1. From the repository, download/transfer 4 No. Python scripts and the config file, as detailed above, to a project workspace.<br>

2. Add AWS credentials, cluster endpoint, database and IAM role details to the config file. As per example below...

        [AWS]
        AWS_ACCESS_KEY_ID = SDDFHYJFG7FREWRQQAZXH3DH
        AWS_SECRET_ACCESS_KEY = Pdfgsf45srP+754SDFDSgbfuEbh
        
        [CLUSTER]
        HOST = #TODO example(dwhcluster.cbndkripqrtkx.us-west-2.redshift.amazonaws.com)
        DB_NAME = lndbikehire
        DB_USER = lbhuser
        DB_PASSWORD = Passw0rd
        DB_PORT = 5439
        
        [IAM_ROLE]
        ARN ='arn:aws:iam::096836204836:role/dwhRole'

3. Open a terminal window to your workspace and change directory to where the project files are located.<br>
   
        C:\users\username>cd C:\users\username\path\to\project
   
4. Run first Python script to create table schema on Redshift cluster... *create_tables.py*;<br>

        C:\users\username>cd C:\users\username\path\to\project>python3 create_tables.py

5. Run second python script to process S3 hosted JSON files to staging tables and final star schema tables... *etl.py*;<br>

        C:\users\username>cd C:\users\username\path\to\project>python3 etl.py 

---
