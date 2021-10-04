import sys

from pyspark.sql import SparkSession

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SparkSqlExample") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Input File is missing")
        sys.exit(-1)

    surveyDF = spark.read \
        .csv(sys.argv[1], header="true", inferSchema=True)

    # using sql expression
    # only we can execute sql expression on table or view

    # create a view of Data frame
    surveyDF.createOrReplaceTempView("survey_tbl")

    # run the query over the view, it will return Data Frame
    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age < 40 group by Country")

    countDF.show()
