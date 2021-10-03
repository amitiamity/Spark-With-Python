from pyspark.sql.session import SparkSession

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    # set config
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting Basic-Example")

    # print all configuration
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    # do the business logic
    logger.info("Finished Basic-Example")

    spark.stop()
