from pyspark.sql import SparkSession

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchema") \
        .getOrCreate()

    logger = Log4J(spark)

    # read csv file
    flightDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", True) \
        .load("data//flight*.csv")

    flightDF.show(5)
    logger.info("CSV Schema: " + flightDF.schema.simpleString())

    # read json file
    # default infer schema is true, so we can avoid it
    flightJsonDF = spark.read \
        .format("json") \
        .option("inferSchema", True) \
        .load("data//flight*.json")

    flightJsonDF.show(5)
    logger.info("JSON Schema: " + flightJsonDF.schema.simpleString())

    # parque. it is a binary file and comes with schema

    flightParquetDF = spark.read \
        .format("parquet") \
        .load("data//flight*.parquet")

    flightParquetDF.show(5)
    logger.info("Parquet Schema: " + flightParquetDF.schema.simpleString())
