from pyspark.sql.functions import year, month, dayofmonth
import os
s3 = "s3a://shwes3udacapstone/"
WEATHER_DATA_PATH = "data/raw/globaltemperatures/GlobalLandTemperaturesByCity.csv"
input_log_data_file = os.path.join(s3, WEATHER_DATA_PATH)
df_weather = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(input_log_data_file)
df_weather_us = df_weather.filter("Country == 'United States'")
df_weather_us = df_weather_us.select(df_weather_us.dt.alias("date"),
                year("dt").alias("Year"),
                month("dt").alias("Month"),
                dayofmonth("dt").alias("DayOfMonth"),
                df_weather_us.AverageTemperature,
                df_weather_us.AverageTemperatureUncertainty,
                df_weather_us.City,
                df_weather_us.Latitude,
                df_weather_us.Longitude)
df_weather_us.repartition("City","Month").write.partitionBy("City","Month").mode("overwrite").parquet(s3 + 'data/processed/weather/')
