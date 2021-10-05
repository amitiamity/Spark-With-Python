from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkDataSink") \
        .getOrCreate()

    logger = Log4J(spark)

    flightParquetDF = spark.read \
        .format("parquet") \
        .load("data//flight*.parquet")

    # log number of partition I have
    logger.info("Number of partition  before :" + str(flightParquetDF.rdd.getNumPartitions()))

    flightParquetDF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightParquetDF.repartition(5)

    # log number of partition I have
    logger.info("Number of partition  after :" + str(partitionedDF.rdd.getNumPartitions()))

    partitionedDF.groupBy(spark_partition_id()).count().show()

    # write avro format. it will clean the directory and create a new file

    ''' partitionedDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save()
    '''

    # partition data by column name
    # and limit number of records per file
    flightParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "datasink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()
