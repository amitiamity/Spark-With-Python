import sys

from pyspark.sql import SparkSession

from lib.logger import Log4J
from lib.utils import load_survey_df, count_by_country

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
    # applying transformation, group by country
    count_df = count_by_country(survey_df)
    # count_df.show()

    # log data as collect
    logger.info(count_df.collect())
    logger.info("Finished execution")
