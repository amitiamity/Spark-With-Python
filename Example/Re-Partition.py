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

    # forcefully repartitioning the data frame

    partitioned_survey_df = survey_df.repartition(2)

    # applying transformation, group by country
    count_df = count_by_country(partitioned_survey_df)
    # count_df.show()

    # log data as collect
    logger.info(count_df.collect())

    # added below line to avoid terminating of Spark UI (as it is only available during life of application
    # to understand the execution plan
    input("Press Enter")
    logger.info("Finished execution")
