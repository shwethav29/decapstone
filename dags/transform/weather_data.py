from pyspark.sql.functions import year, month, dayofmonth, udf
from pyspark.sql.types import StringType

import os
s3 = "s3a://shwes3udacapstone/"
WEATHER_DATA_PATH = "data/raw/globaltemperatures/GlobalLandTemperaturesByCity.csv"
input_log_data_file = os.path.join(s3, WEATHER_DATA_PATH)
udf_capitalize_lower = udf(lambda x:str(x).lower().capitalize(),StringType())

df_weather = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(input_log_data_file)
df_weather_us = df_weather.filter("Country == 'United States'")
df_weather_us = df_weather_us.select(df_weather_us.dt.alias("date"),
                year("dt").alias("Year"),
                month("dt").alias("Month"),
                dayofmonth("dt").alias("DayOfMonth"),
                df_weather_us.AverageTemperature,
                df_weather_us.AverageTemperatureUncertainty,
                udf_capitalize_lower("City").alias("city"),
                df_weather_us.Latitude,
                df_weather_us.Longitude)
df_weather_us.repartition("Month").write.partitionBy("Month").mode("overwrite").parquet(s3 + 'data/processed/weather/')
