from pyspark.sql.functions import year, month, dayofmonth
output_data = "s3a://shwes3udacapstone/"
df_weather = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("../../data2/GlobalLandTemperaturesByCity.csv")
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
df_weather_us.partitionBy("City,Month,DayOfMonth").mode("overwrite").parquet(output_data + 'data/processed/weather/')
