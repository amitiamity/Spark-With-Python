import sys

from pyspark.sql import SparkSession
from lib.logger import Log4J
from lib.utils import load_survey_df

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Read-File-Sample") \
        .getOrCreate()

    logger = Log4J(spark)
    if len(sys.argv) != 2:
        logger.error("Input sample is required")
        sys.exit(-1)

    survey_df = load_survey_df(spark, sys.argv[1])
    survey_df.show()

    logger.info("Finished execution")
