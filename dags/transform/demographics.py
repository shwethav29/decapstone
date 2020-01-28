from pyspark.sql.functions import year, month, dayofmonth
import os
s3 = "s3a://shwes3udacapstone/"
DEMOGRAPHICS_DATA_PATH = "data/raw/demographics/us-cities-demographics.csv"
input_log_data_file = os.path.join(s3, DEMOGRAPHICS_DATA_PATH)

df_demo = spark.read.format("csv").option("delimiter", ";").option("header", "true").load(input_log_data_file)
df_demo = df_demo.withColumnRenamed("State Code","state_code").withColumnRenamed("Median Age","median_age").withColumnRenamed("City","city").withColumnRenamed("Total Population","population")
df_demo = df_demo.select("city","median_age","population","state_code")
df_state = spark.read.parquet(s3+"data/processed/codes/us_state")
df_demo = df_demo.join(df_state,"state_code")
df_demo.write.mode("overwrite").parquet(s3 + 'data/processed/city/')