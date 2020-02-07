from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
s3 = "s3a://shwes3udacapstone/"
IMMIGRATION="data/processed/immigration/"
IMMIGRANT="data/processed/immigrant/"
IMMIGRANT_CITY="data/processed/immigration_demo_weather/"

spark.sparkContext.setLogLevel("WARN")

Logger= spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger("DAG")


def check(path, table):
    df = spark.read.parquet(path).filter("i94dt = '{}'".format(year_month))
    if len(df.columns) > 0 and df.count() > 0:
        mylogger.warn("{} SUCCESS".format(table))
    else:
        mylogger.warn("{} FAIL".format(table))



check(s3+IMMIGRATION,"immigration")
check(s3+IMMIGRANT,"immigrant")
check(s3+IMMIGRANT_CITY,"immigration_demo_weather")