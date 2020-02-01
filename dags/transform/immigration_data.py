from pyspark.sql import SparkSession
import datetime as dt
import pyspark.sql.functions as F
#"saurfang:spark-sas7bdat:2.1.0-s_2.11"
spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
.enableHiveSupport().getOrCreate()

udf_parse_arrival_dt = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)


# process and clean data from i94 label description file
def process_state_codes():
    df_state = spark.read.format("csv").option("delimiter", "=").option("header", "False").load("./i94addrl.txt")
    df_state = df_state.withColumnRenamed("_c0", "state_code").withColumnRenamed("_c1", "state_name")
    df_state = df_state.withColumn("state_code", F.regexp_replace(df_state.state_code, "[^A-Z]", ""))
    df_state = df_state.withColumn("state_name", F.regexp_replace(df_state.state_name, "'", ""))
    df_state = df_state.withColumn("state_name", F.ltrim(F.rtrim(df_state.state_name)))
    return df_state


def process_country_codes():
    df_country = spark.read.format("csv").option("delimiter", "=").option("header", "False").load("./i94cntyl.txt")
    df_country = df_country.withColumnRenamed("_c0", "country_code").withColumnRenamed("_c1", "country_name")
    df_country = df_country.withColumn("country_name", F.regexp_replace(df_country.country_name, "'", ""))
    df_country = df_country.withColumn("country_name", F.ltrim(F.rtrim(df_country.country_name)))
    df_country = df_country.withColumn("country_code", F.ltrim(F.rtrim(df_country.country_code)))
    df_country = df_country.withColumn("country_name",
                                       F.regexp_replace(df_country.country_name, "^INVALID.*|Collapsed.*|No\ Country.*",
                                                        "INVALID"))
    df_country.count()
    return df_country


def process_port_codes():
    df_port = spark.read.format("csv").option("delimiter", "=").option("header", "False").load("./i94prtl.txt")
    df_port = df_port.withColumn("_c0", F.regexp_replace(df_port._c0, "'", "")).withColumn("_c1",
                                                                                           F.regexp_replace(df_port._c1,
                                                                                                            "'", ""))
    split_col = F.split(df_port._c1, ",")
    df_port = df_port.withColumn("city", split_col.getItem(0))
    df_port = df_port.withColumn("state_code", split_col.getItem(1))
    df_port = df_port.withColumnRenamed("_c0", "port_code")
    df_port = df_port.drop("_c1")
    df_port = df_port.withColumn("port_code", F.regexp_replace(df_port.port_code, "[^A-Z]", "")).withColumn("city",
                                                                                                            F.ltrim(
                                                                                                                F.rtrim(
                                                                                                                    df_port.city))).withColumn(
        "state_code", F.regexp_replace(df_port.state_code, "[^A-Z]", ""))
    df_state = process_state_codes()
    df_port = df_port.join(df_state, "state_code")
    df_port.show(5)
    return df_port



from pyspark.sql import SparkSession
import datetime as dt
import pyspark.sql.functions as F

spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
.enableHiveSupport().getOrCreate()

udf_parse_arrival_dt = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

df_spark =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
df_spark = df_spark.withColumn("arrdate",udf_parse_arrival_dt(df_spark.arrdate))
df_spark = df_spark.withColumn("depdate",udf_parse_arrival_dt(df_spark.depdate))
df_i94mode = spark.read.format("csv").option("delimiter","=").option("header","False").load("./i94model.txt")

df_i94mode=df_i94mode.withColumn("_c0", F.regexp_replace(df_i94mode._c0, "'", "")).\
    withColumn("_c1",F.regexp_replace(df_i94mode._c1,"[^A-Za-z]", ""))
df_i94mode = df_i94mode.withColumnRenamed("_c0", "i94mode").withColumnRenamed("_c1","mode")
df_spark = df_spark.withColumnRenamed("i94yr","year").withColumnRenamed("i94mon","month").withColumnRenamed("i94cit","birth_country")\
    .withColumnRenamed("i94res","residence_country").withColumnRenamed("i94port","port_of_entry").\
    withColumnRenamed("arrdate","arrival_date").\
    withColumnRenamed("depdate","departure_date").withColumnRenamed("i94bir","age")
df_spark = df_spark.join(F.broadcast(df_i94mode),["i94mode"])
df_spark.show(5)