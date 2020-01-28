import os
import pyspark.sql.functions as F
output_data = "s3a://shwes3udacapstone/"

s3 = "s3a://shwes3udacapstone/"
I94_CODES_DATA_PATH = "data/raw/codes/"
AIRPORT_FILE="i94prtl.txt"
COUNTRY_FILE="i94cntyl.txt"
STATE_FILE="i94addrl.txt"

# process and clean data from i94 label description file
def process_state_codes():
    input_data_file = os.path.join(s3, I94_CODES_DATA_PATH+STATE_FILE)
    df_state = spark.read.format("csv").option("delimiter", "=").option("header", "False").load(input_data_file)
    df_state = df_state.withColumnRenamed("_c0", "state_code").withColumnRenamed("_c1", "state_name")
    df_state = df_state.withColumn("state_code", F.regexp_replace(df_state.state_code, "[^A-Z]", ""))
    df_state = df_state.withColumn("state_name", F.regexp_replace(df_state.state_name, "'", ""))
    df_state = df_state.withColumn("state_name", F.ltrim(F.rtrim(df_state.state_name)))
    df_state.write.mode("overwrite").parquet(s3 + 'data/processed/codes/us_state')
    return df_state


def process_country_codes():
    input_data_file = os.path.join(s3, I94_CODES_DATA_PATH + COUNTRY_FILE)
    df_country = spark.read.format("csv").option("delimiter", "=").option("header", "False").load(input_data_file)
    df_country = df_country.withColumnRenamed("_c0", "country_code").withColumnRenamed("_c1", "country_name")
    df_country = df_country.withColumn("country_name", F.regexp_replace(df_country.country_name, "'", ""))
    df_country = df_country.withColumn("country_name", F.ltrim(F.rtrim(df_country.country_name)))
    df_country = df_country.withColumn("country_code", F.ltrim(F.rtrim(df_country.country_code)))
    df_country = df_country.withColumn("country_name",
                                       F.regexp_replace(df_country.country_name, "^INVALID.*|Collapsed.*|No\ Country.*",
                                                        "INVALID"))
    df_country.write.mode("overwrite").parquet(s3 + 'data/processed/codes/country')
    return df_country


def process_airport_codes():

    #transform airport codes
    input_data_file = os.path.join(s3, I94_CODES_DATA_PATH + AIRPORT_FILE)
    df_airport = spark.read.format("csv").option("delimiter", "=").option("header", "False").load(input_data_file)
    df_airport = df_airport.withColumn("_c0", F.regexp_replace(df_airport._c0, "'", "")).withColumn("_c1",
                                                                                                  F.regexp_replace(
                                                                                                      df_airport._c1,
                                                                                                      "'", ""))
    split_col = F.split(df_airport._c1, ",")
    df_airport = df_airport.withColumn("city", split_col.getItem(0))
    df_airport = df_airport.withColumn("state_code", split_col.getItem(1))
    df_airport = df_airport.withColumnRenamed("_c0", "airport_code")
    df_airport = df_airport.drop("_c1")
    df_airport = df_airport.withColumn("airport_code",
                                       F.regexp_replace(df_airport.airport_code, "[^A-Z]", "")).withColumn("city",
                                                                                                           F.ltrim(
                                                                                                               F.rtrim(
                                                                                                                   df_airport.city))).withColumn(
        "state_code", F.regexp_replace(df_airport.state_code, "[^A-Z]", ""))
    df_state = process_state_codes()
    df_airport = df_airport.join(df_state, df_airport.state_code == df_state.state_code)
    df_airport.write.mode("overwrite").parquet(s3 + 'data/processed/codes/us_ports')

process_airport_codes()
process_country_codes()


