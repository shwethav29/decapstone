s3 = "s3a://shwes3udacapstone/"
IMMIGRATION="data/processed/immigration/"
IMMIGRANT="data/processed/immigrant/"
IMMIGRANT_CITY="data/processed/immigration_demographics/"

spark.sparkContext.setLogLevel("WARN")

Logger= spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger("DAG")


def check(path, table):
    df = spark.read.parquet(path).filter("i94dt = '{}'".format(year_month))
    if len(df.columns) > 0 and df.count() > 0:
        mylogger.warn("{} SUCCESS".format(table))
    else:
        raise ValueError("Data quality checks failed")



check(s3+IMMIGRATION,"immigration")
check(s3+IMMIGRANT,"immigrant")
check(s3+IMMIGRANT_CITY,"immigration_demographics")