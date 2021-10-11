import configparser
import os

from pyspark.sql import SparkSession
from math import radians, sin, cos, acos
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, dayofweek
from pyspark.sql.functions import to_timestamp, to_date, unix_timestamp
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, DateType as Date, TimestampType

config = configparser.ConfigParser()
config.read('aws/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Builds a Spark session"""

    spark = SparkSession \
        .builder \
        .appName("LndBikeHireDataProcessing") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_docking_station_data(spark, input_data, output_data):
    """Processes bike docking station data to AWS S3 hosted parquet file

    - Reads in docking station data from a csv file and builds a schema defined docking station dataframe.
    - Writes 'docking stations' dimension table as parquet file to AWS S3 data lake.

    :param spark: (object): Built spark session instance
    :param input_data: (str): Path to AWS S3 bucket source data
    :param output_data: (str): Path to AWS S3 bucket of written parquet file
    """

    # sets filepath to docking station data file
    station_data = os.path.join(input_data, 'infrastructure/FOI-0689-2122.csv')

    # set docking station table schema
    docking_schema = R([
        Fld('docking_station_live_date', Str()),
        Fld('docking_station_name', Str()),
        Fld('docking_station_id', Int()),
        Fld('docking_points', Int()),
        Fld('docking_station_latitude', Dbl()),
        Fld('docking_station_longitude', Dbl()),
    ])
    # read docking station data to dataframe
    dim_docking_stations = spark.read.option("header", True).csv(station_data, docking_schema)

    # write docking station table to parquet file
    dim_docking_stations.write.mode("overwrite").parquet(output_data + 'infrastructure/docking_stations')


def process_journey_data(spark, input_data, output_data):
    """Processes bike hire data to partitioned parquet files

    - Reads in bike hire journey data from csv files to schema defined staging dataframe.
    - Builds a 'Time' dimension table from selected selected staging dataframe columns and writes table to partitioned
      parquet files.
    - Builds a 'Distances' dimension table from selected selected staging dataframe columns and imported docking station
      data, then writes table to a parquet file.
    - Builds a 'Journeys' fact table from selected selected staging dataframe columns and writes table to partitioned
      parquet files.

    :param spark: (object): Built spark session instance
    :param input_data: (str): Path to AWS S3 bucket source data
    :param output_data: (str): Path to AWS S3 bucket of written partitioned parquet files
    """

    # sets filepath to bike hire data files - Select 1 of 3 options below
    # journey_data = os.path.join(input_data, 'journey/2012/11. Journey Data Extract 23Aug-25 Aug12.csv')
    journey_data = os.path.join(input_data, 'journey/2012/*.csv')
    # journey_data = os.path.join(input_data, 'journey/*/*.csv')

    journey_schema = R([
        Fld('Rental Id', Int(), True),
        Fld('Duration', Int()),
        Fld('Bike Id', Int()),
        Fld('End Date', Str()),
        Fld('EndStation Id', Int()),
        Fld('EndStation Name', Str()),
        Fld('Start Date', Str()),
        Fld('StartStation Id', Int()),
        Fld('StartStation Name', Str()),
    ])

    # reads journey data files to spark dataframe
    raw_df = spark.read.option("header", True).csv(journey_data, journey_schema)

    # Staging Transforms: Timestamp conversions | Duration filter | Start Date Filter | Drop duplicates
    df_staging = raw_df.withColumn("Start Date", unix_timestamp("Start Date", 'dd/MM/yyyy HH:mm').cast(TimestampType())) \
        .withColumn("End Date", unix_timestamp("End Date", 'dd/MM/yyyy HH:mm').cast(TimestampType())) \
        .filter(col("Start Date") > "2012-01-01 00:00:00") \
        .dropDuplicates(["Rental Id"]) \
        .withColumnRenamed("StartStation Id", "start_station_id") \
        .withColumnRenamed("EndStation Id", "end_station_id") \
        .withColumn("rental_start_year", year("Start Date")) \
        .withColumn("rental_start_month", month("Start Date")) \
        .withColumn("rental_start_day", dayofmonth("Start Date")) \
        .withColumnRenamed("Start Date", "rental_start_date") \
        .withColumnRenamed("End Date", "rental_end_date")

    # extract columns to create a time dimension table
    dim_time_table = df_staging.select('rental_start_date') \
        .withColumn('hour', hour('rental_start_date')) \
        .withColumn('day', dayofmonth('rental_start_date')) \
        .withColumn('weekday', dayofweek('rental_start_date')) \
        .withColumn('week', weekofyear('rental_start_date')) \
        .withColumn('month', month('rental_start_date')) \
        .withColumn('year', year('rental_start_date'))

    # write time table to parquet files partitioned by year and month
    dim_time_table.withColumn('month_', month('rental_start_date')) \
        .withColumn('year_', year('rental_start_date')) \
        .repartition(10) \
        .write.partitionBy('year_', 'month_') \
        .mode("overwrite") \
        .parquet(output_data + 'time')

    fact_journeys_table = df_staging.select([col("Rental Id").alias("rental_id"),
                                             col("Bike Id").alias("bike_id"),
                                             col("Duration").alias("rental_duration_seconds"),
                                             col("start_station_id"),
                                             col("rental_start_date"),
                                             col("end_station_id"),
                                             col("rental_end_date")]) \
        .withColumn("rental_start_year", year("rental_start_date")) \
        .withColumn("rental_start_month", month("rental_start_date")) \
        .withColumn("rental_start_day", dayofmonth("rental_start_date"))

    # write journeys fact table to parquet files partitioned by year and month
    fact_journeys_table.withColumn("rental_start_year_", col("rental_start_year")) \
        .withColumn("rental_start_month_", col("rental_start_month")) \
        .repartition(10) \
        .write.partitionBy('rental_start_year_', 'rental_start_month_') \
        .mode("overwrite") \
        .parquet(output_data + 'journeys')


def journey_distance(spark, output_infrastructure_data, output_data):
    """Processes journey and docking station data to create a dimension table of calculated journey distances

    - Reads docking station id and location coordinates data from processed parquet file, Creates dataframe.
    - Reads journey rental id and start/end station id from processed parquet files, Creates dataframe.
    - Creates start and end station dataframes, each combining station ids and coordinates
    - Creates merged dataframe of start and end station ids, coordinates and calculated journey distance.

    :param spark: (object): Built spark session instance
    :param output_infrastructure_data: (str): Path to processed docking station data
    :param output_data: (str): Path to processed journey data
    """

    def journey_distance_calc(start_lat, start_lon, end_lat, end_lon):
        """Inner function - Calculates distance between journey start and end station coordinates

        Parameters:
        start_lat (float): Start station latitude
        start_lon (float): Start station longitude
        end_lat (float): End station latitude
        end_lon (float): End station longitude
        returns:
        distance (float): Kilometer distance between stations
        """

        slat = radians(float(start_lat))
        slon = radians(float(start_lon))
        elat = radians(float(end_lat))
        elon = radians(float(end_lon))

        if slat == elat and slon == elon:
            dist = 0
        else:
            distance = 6371.01 * acos(sin(slat) * sin(elat) + cos(slat) * cos(elat) * cos(slon - elon))
            return round(distance, 2)

    # Read in processed docking station data from parquet file
    stations_df = spark.read.parquet(output_data + 'infrastructure/docking_stations') \
        .select('docking_station_id', 'docking_station_latitude', 'docking_station_longitude')

    # Read in processed journey data from parquet files
    journeys_df = spark.read.parquet(output_data + 'journeys') \
        .select('rental_id', 'start_station_id', 'end_station_id',
                'rental_start_day', 'rental_start_month', 'rental_start_year') \
        .filter('end_station_id > 0')

    # Create starting station dataframe for journey distance calculation
    start_station_df = journeys_df.join(stations_df, (journeys_df.start_station_id == stations_df.docking_station_id),
                                        'left') \
        .select(col("rental_id"),
                col("start_station_id"),
                col("docking_station_latitude").alias("start_lat"),
                col("docking_station_longitude").alias("start_lon"),
                col("rental_start_year"),
                col("rental_start_month"),
                col("rental_start_day")) \
        .dropDuplicates(["rental_id"])

    # Create ending station dataframe for journey distance calculation
    end_station_df = journeys_df.join(stations_df, (journeys_df.end_station_id == stations_df.docking_station_id),
                                      'left') \
        .select(col("rental_id"),
                col("end_station_id"),
                col("docking_station_latitude").alias("end_lat"),
                col("docking_station_longitude").alias("end_lon")) \
        .dropDuplicates(["rental_id"])

    # Create journey distances dataframe from joined starting/ending dataframes
    distances_df = start_station_df.join(end_station_df, ["rental_id"]).dropna()

    # TEST WRITE -- DELETE--
    # distances_df.withColumn("rental_start_year_", col("rental_start_year")) \
    # .repartition(10) \
    # .write.partitionBy('rental_start_year_') \
    # .mode('overwrite') \
    # .parquet(output_data + 'test')

    # Add column to dataframe of calculated distances
    distances_udf = udf(journey_distance_calc, Dbl())
    dim_journey_distances = distances_df.withColumn("journey_distance_km",
                                                    distances_udf(distances_df['start_lat'], distances_df['start_lon'],
                                                                  distances_df['end_lat'], distances_df['end_lon']))

    # Write journey distances table to parquet files partitioned by year and month
    dim_journey_distances.withColumn("rental_start_year_", col("rental_start_year")) \
        .withColumn("rental_start_month_", col("rental_start_month")) \
        .repartition(10) \
        .write.partitionBy('rental_start_year_', 'rental_start_month_') \
        .mode('overwrite') \
        .parquet(output_data + 'journey_distances')


def process_weather_data(spark, input_data, output_data):
    """Processes weather data from json files to partitioned parquet files

    :param spark:(object): Built spark session instance
    :param input_data: (str): Path to AWS S3 bucket source data
    :param output_data: (str): Path to AWS S3 bucket of written partitioned parquet files
    """

    # sets filepath to bike hire data files
    weather_data_path = os.path.join(input_data, 'weather/*.json')

    # reads weather data files to dataframe
    raw_weather_df = spark.read.option("multiLine", "true").format("json").load(weather_data_path)

    # create daily weather dataframe from nested json
    days_weather_df = raw_weather_df.select(explode('days').alias('day'))

    # split daily weather array to individual columns
    dim_daily_weather_table = days_weather_df.withColumn('date', days_weather_df.day.getItem('datetime').cast('date')) \
        .withColumn("year", year('date')) \
        .withColumn('month', month('date')) \
        .withColumn('day_of_month', dayofmonth('date')) \
        .withColumn('conditions', days_weather_df.day.getItem('conditions')) \
        .withColumn('description', days_weather_df.day.getItem('description')) \
        .withColumn('avg_temp', days_weather_df.day.getItem('temp')) \
        .withColumn('min_temp', days_weather_df.day.getItem('tempmin')) \
        .withColumn('max_temp', days_weather_df.day.getItem('tempmax')) \
        .withColumn('precipitation', days_weather_df.day.getItem('precip')) \
        .withColumn('windspeed', days_weather_df.day.getItem('windspeed')) \
        .withColumn('sunrise', days_weather_df.day.getItem('sunrise')) \
        .withColumn('sunset', days_weather_df.day.getItem('sunset')) \
        .drop('day')

    # write daily weather table to parquet files  partitioned by year and month
    dim_daily_weather_table.withColumn("year_", col('year')) \
        .withColumn('month_', col('month')) \
        .repartition(10) \
        .write.partitionBy('year_', 'month_') \
        .mode("overwrite") \
        .parquet(output_data + 'weather')


def main():
    """Main script function"""

    spark = create_spark_session()
    input_data = config.get("S3", "INPUT_DATA")
    output_data = config.get("S3", "OUTPUT_DATA")

    print('Processing docking station data...')
    process_docking_station_data(spark, input_data, output_data)
    print('Docking station data processing complete!')

    print('Processing journey data...')
    process_journey_data(spark, input_data, output_data)
    print('Journey data processing complete!')

    print('Calculating journey distances...')
    journey_distance(spark, output_data, output_data)
    print('Journey distance calculations complete!')

    print('Processing weather data...')
    process_weather_data(spark, input_data, output_data)
    print('Weather data processing complete!')

    print('Woo... All Data Processed!')


if __name__ == "__main__":
    main()
