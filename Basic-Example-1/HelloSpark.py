from pyspark.sql import SparkSession
from lib.logger import Log4J

if __name__ == "__main__":
    # spark session is a driver manager
    spark = SparkSession.builder\
        .appName("Basic-Example-1")\
        .master("local[3]")\
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting Basic-Example-1")

    # do the business logic
    logger.info("Finished Basic-Example-1")

    spark.stop()
