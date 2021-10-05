from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaProgramatically") \
        .getOrCreate()

    logger = Log4J(spark)

    # define schema data types using struct type
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    # read csv file
    """
    : schema to be referred
    : mode in case invalid data comes
    : data format for inferring date fields
    """
    flightDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data//flight*.csv")

    flightDF.show(5)
    logger.info("CSV Schema: " + flightDF.schema.simpleString())

    # DDL type data types schema
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""
    # read json file
    # default infer schema is true, so we can avoid it
    flightJsonDF = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .load("data//flight*.json")

    flightJsonDF.show(5)
    logger.info("JSON Schema: " + flightJsonDF.schema.simpleString())

    # parque. it is a binary file and comes with schema

    flightParquetDF = spark.read \
        .format("parquet") \
        .load("data//flight*.parquet")

    flightParquetDF.show(5)
    logger.info("Parquet Schema: " + flightParquetDF.schema.simpleString())
