import datetime as dt
import pyspark.sql.functions as F
import os
s3 = "s3a://shwes3udacapstone/"

df_immigration = spark.read.parquet(s3+"data/processed/immigration/").filter("i94dt = '{}'".format(year_month))
df_immigrant = spark.read.parquet(s3+"data/processed/immigrant/").filter("i94dt = '{}'".format(year_month))
df_weather = spark.read.parquet(s3+"data/processed/weather/").filter("Month = '{}' and Year = '{}'".format(year_month[-2],year_month[0:4]))
df_airport=spark.read.parquet(s3+"data/processed/airports/").filter("Month = '{}'".format(year_month[-2]))
df_demo = spark.read.parquet(s3 + 'data/processed/city/')
df_weather_airport = df_airport.join(df_weather("AverageTemperature"),[df_airport.city==df_weather.City])
df_weather_airport = df_weather_airport.join(df_demo("median_age","population","state_code"),[df_weather_airport.city==df_demo.city])

df_immigration_weather_demo = df_immigration.join(df_weather_airport,[df_immigration.port_of_entry==df_weather_airport.airport_code])

df_immigration_weather_demo = df_immigration_weather_demo.join(df_immigrant,["cicid"])

df_immigration_weather_demo.repartition("i94dt").write.partitionBy("i94dt").mode("append").parquet(s3 + 'data/processed/immigration_demo_weather/')






