import logging
from datetime import datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_utc_timestamp, unix_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, FloatType, LongType

import time

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.current_weather (
        id UUID PRIMARY KEY,
        timestamputc INT,
        timezone INT,
        location TEXT,
        weather TEXT,
        weather_description TEXT,
        temperature FLOAT,
        pressure INT,
        humidity INT,
        wind_speed FLOAT,
        clouds INT,
        precipitation FLOAT);
    """)

    print("Table created successfully!")

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        print("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'airflow-spark') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
        print("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()
        print("Cassandra connection created successfully")
        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamputc", IntegerType(), True),
        StructField("timezone", IntegerType(), True),
        StructField("location", StringType(), True),
        StructField("weather", StringType(), True),
        StructField("weather_description", StringType(), True),
        StructField("temperature", FloatType(), True),
        #StructField("thermal_sensation", FloatType(), True),
        #StructField("temp_max", FloatType(), True),
        #StructField("temp_min", FloatType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        #StructField("visibility", IntegerType(), True),
        StructField("wind_speed", FloatType(), True),
        #StructField("wind_deg", IntegerType(), True),
        #StructField("wind_gust", FloatType(), True),
        StructField("clouds", IntegerType(), True),
        StructField("precipitation", FloatType(), True)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    print(sel)

    return sel

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
    time.sleep(2)

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            logging.info("Streaming is being started...")
            print("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'current_weather')
                               .option("failOnDataLoss", "false")
                               .start())

            streaming_query.awaitTermination()