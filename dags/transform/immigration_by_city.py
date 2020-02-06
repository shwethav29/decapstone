import datetime as dt
import pyspark.sql.functions as F
import os
s3 = "s3a://shwes3udacapstone/"

df_immigration = spark.read.parquet(s3+"data/processed/immigration/").filter("i94dt = '{}'".format(year_month))
df_immigrant = spark.read.parquet(s3+"data/processed/immigrant/").filter("i94dt = '{}'".format(year_month)).drop("i94dt")
df_weather = spark.read.parquet(s3+"data/processed/weather/").filter("Month = '{}' and Year = '{}'".format(year_month[-2],year_month[0:4]))
df_weather = df_weather.selectExpr("City","AverageTemperature")
df_airport=spark.read.parquet(s3+"data/processed/airports/")
df_demo = spark.read.parquet(s3 + 'data/processed/city/')
df_weather_airport = df_airport.join(df_weather,[df_airport.city==df_weather.City])
df_weather_airport = df_weather_airport.join(df_demo,["city"])
df_weather_airport = df_weather_airport.selectExpr("city","airport_code","ident","name","local_code","AverageTemperature","median_age","population")
df_immigration_weather_demo = df_immigration.join(df_weather_airport,[df_immigration.port_of_entry==df_weather_airport.airport_code]).drop("port_of_entry")

df_immigration_weather_demo = df_immigration_weather_demo.join(df_immigrant,["cicid"])

df_immigration_weather_demo.repartition("i94dt").write.partitionBy("i94dt").mode("append").parquet(s3 + 'data/processed/immigration_demo_weather/')






