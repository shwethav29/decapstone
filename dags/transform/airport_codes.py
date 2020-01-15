# Do all imports and installs here
import configparser
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, DoubleType, StringType
config = configparser.ConfigParser()
config.read('./dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

spark = SparkSession.builder\
                     .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
                     .getOrCreate()

output_data = "s3a://shwes3udacapstone/"

# Read in the data here
# Read in the data here
def parseUDFlat(coordinates):
    l=coordinates.strip().split(",",1)
    return float(l[0])

udf_parse_lat = udf(lambda x: parseUDFlat(x), DoubleType())

def parseUDFlog(coordinates):
    l=coordinates.strip().split(",",1)
    return float(l[1])

udf_parse_log = udf(lambda x: parseUDFlog(x), DoubleType())

def parse_state(iso_region):
    state = None
    if(iso_region is not None):
        l = iso_region
        l=iso_region.strip().split("-",1)
        state = l[1]
    return state
udf_parse_state = udf(lambda x:parse_state(x),StringType())

df = spark.read.format("csv").option("header", "true").load("./airport-codes_csv.csv")
df = df.filter("iso_country='US' and type!='closed' and iata_code!='null'")
#df.select("type").distinct().show()
#df.select("iata_code").distinct().show()
df_ltlg = df.withColumn("latitude",udf_parse_lat("coordinates")).withColumn("longitude",udf_parse_log("coordinates"))
df_state = df_ltlg.withColumn("state",udf_parse_state("iso_region"))
columns = ["ident","type","name","elevation_ft","gps_code","iata_code","local_code","latitude","longitude"]
df_city = df_state.selectExpr("municipality as city","state").dropDuplicates()
df_city.write.mode("overwrite").parquet(output_data + 'city/')
df_airport = df_state.select(*columns)
df_airport.show(5)
