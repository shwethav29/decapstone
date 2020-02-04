import datetime as dt
import pyspark.sql.functions as F
import os
s3 = "s3a://shwes3udacapstone/"
I94_CODES_DATA_PATH = "data/raw/codes/"
AIRPORT_FILE="i94prtl.txt"
COUNTRY_FILE="i94cntyl.txt"
STATE_FILE="i94addrl.txt"

udf_parse_arrival_dt = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

IMMIGRATION_DATA = "data/raw/i94_immigration_data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat"
input_log_data_file = os.path.join(s3, IMMIGRATION_DATA)

df_spark =spark.read.format('com.github.saurfang.sas.spark').load(input_log_data_file)
df_spark = df_spark.withColumn("arrdate",udf_parse_arrival_dt(df_spark.arrdate))
df_spark = df_spark.withColumn("depdate",udf_parse_arrival_dt(df_spark.depdate))


df_i94mode = spark.read.parquet(s3+"data/processed/codes/i94mode")
df_state = spark.read.parquet(s3+"data/processed/codes/us_state")
df_country = spark.read.parquet(s3+"data/processed/codes/country")
df_i94visa = spark.read.parquet(s3+"data/processed/codes/i94visa")


df_spark = df_spark.withColumnRenamed("i94yr","year").withColumnRenamed("i94mon","month")\
    .withColumnRenamed("i94port","port_of_entry")\
    .withColumnRenamed("arrdate","arrival_date")\
    .withColumnRenamed("depdate","departure_date").withColumnRenamed("i94bir","age")
df_spark = df_spark.join(F.broadcast(df_i94mode),["i94mode"])
df_spark = df_spark.filter("mode == 'Air'")


df_spark = df_spark.join(F.broadcast(df_i94visa),["i94visa"])
df_spark = df_spark.join(F.broadcast(df_state),[df_spark.i94addr == df_state.state_code])
df_spark = df_spark.join(F.broadcast(df_country),[df_spark.i94res==df_country.country_code])
df_spark = df_spark.drop("i94visa","i94mode","i94addr","i94res","country_code")
df_spark = df_spark.withColumnRenamed("country_name","residence_country")
df_spark = df_spark.join(F.broadcast(df_country),[df_spark.i94cit==df_country.country_code])
df_spark = df_spark.drop("i94cit","country_code")
df_spark = df_spark.withColumnRenamed("country_name","birth_country")
df_spark.show(5)
df_immigrant = df_spark.selectExpr('cast(cicid as int) cicid', 'age', 'occup','biryear','birth_country',\
'residence_country','gender','visapost','visa','visatype')
df_immigrant = df_spark.selectExpr('cast(cicid as int) cicid', 'cast(age as int) age', 'occup','cast(biryear as int) birth_year','birth_country',\
'residence_country','gender','visapost','visa','visatype').withColumn("i94dt",F.lit(year_month))
df_immigrant.repartition("i94dt","visa").write.partitionBy("i94dt","visa").mode("overwrite").parquet(s3 + 'data/processed/immigrant/')

df_immigration = df_spark.selectExpr('cast(cicid as int) cicid', 'cast(year as int) year','cast(month as int) month','port_of_entry','arrival_date',\
                                     'departure_date','dtadfile','entdepa','entdepd','entdepu','matflag','dtaddto',\
                                     'insnum','airline','admnum','cast(admnum as long) admnuml','fltno','state_code','state_name').withColumn("i94dt",F.lit(year_month))
df_immigration.repartition("i94dt","state_code").write.partitionBy("i94dt","state_code").mode("overwrite").parquet(s3 + 'data/processed/immigrantion/')
