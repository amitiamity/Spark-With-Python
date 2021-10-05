from pyspark.sql import SparkSession

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTable") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    flight_df = spark.read \
        .format("parquet") \
        .load("data/")

    # spark has default database called 'default'
    # but we can create our own database
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")

    # set current database
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # we can create multile partitions based on they columns name but this can result in 1000 of partitions
    # so we can create buckets
    # we can sort the data based on the column too
    flight_df.write \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    # print all tables in the given db

    logger.info(spark.catalog.listTables("AIRLINE_DB"))
