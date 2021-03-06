from pyspark.sql.functions import udf, trim, lower

s3 = "s3a://shwes3udacapstone/"

df_immigration_airport=spark.read.parquet(s3+"data/processed/airports/")
df_immigration_airport = df_immigration_airport.withColumn("city",lower(df_immigration_airport.city))
df_demo = spark.read.parquet(s3 + 'data/processed/city/')
df_demo_airport = df_immigration_airport.join(df_demo,["city","state_code","state_name"])

df_immigration = spark.read.parquet(s3+"data/processed/immigration/").filter("i94dt=='{0}'".format(year_month))
df_immigration = df_immigration.withColumnRenamed("port_of_entry","airport_code")
df_demo_airport = df_demo_airport.drop("state_code","state_name")
df_immigration_demo = df_immigration.join(df_demo_airport,["airport_code"]).\
selectExpr("cicid","arrival_date","departure_date","airport_code","name","city","state_code","state_name","population","median_age","i94dt")

df_immigrant = spark.read.parquet(s3+"data/processed/immigrant/").filter("i94dt=='{0}'".format(year_month)).drop("i94dt")
df_immigrant_demographics = df_immigrant.join(df_immigration_demo,["cicid"]).\
selectExpr("cicid","age","birth_country","residence_country","gender","visatype","visa",\
           "i94dt","arrival_date","departure_date","airport_code","name","city","state_code",\
           "state_name","population","median_age")

df_immigrant_demographics.write.partitionBy("i94dt").mode("append").parquet(s3 + 'data/processed/immigration_demographics/')





