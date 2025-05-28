# Databricks notebook source
sfOptions = {
  "sfURL" : "https://hl42245.us-east-2.aws.snowflakecomputing.com",
  "sfDatabase" : "END_TO_END",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH",
  "sfUser" : dbutils.secrets.get(scope="snowflake-creds", key="sf-user"),
  "sfPassword" : dbutils.secrets.get(scope="snowflake-creds", key="sf-password")
}

# COMMAND ----------

dbutils.fs.ls('dbfs:/project/curated/2014/')

# COMMAND ----------

df_2014=spark.read.format("parquet").load('dbfs:/project/curated/2014/')
df_2014.columns

# COMMAND ----------

df_2014.select(col("OP_CARRIER")).distinct().show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col,when
df_dim_carrier = df_2014.select(
    col("OP_CARRIER").alias("carrier_code"),
    col("Carrier_name").alias("carrier_name")
).distinct()
df_dim_carrier.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_CARRIER") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_dim_airport=df_2014.select(
    col("origin").alias("origin_airport_code"),
    col("dest").alias("dest_airport_code")
).distinct()
df_dim_airport.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_AIRPORT") \
  .mode("append") \
  .save()

# COMMAND ----------

df_dim_date=df_2014.select(
    col("fl_date").alias("flight_date"),
    col("quarter").alias("quarter"),
    col("is_weekend").alias("is_weekend")
).distinct()
df_dim_date.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_DATE") \
  .mode("append") \
  .save()

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

df_fact_flights = df_2014.select(
    col("fl_date").alias("flight_date"),
    col("op_carrier_fl_num").alias("op_carrier_fl_num"),
    col("op_carrier").alias("carrier_code"),
    col("origin").alias("origin_airport_code"),
    col("dest").alias("dest_airport_code"),
    col("crs_dep_time").alias("crs_dep_time"),
    col("dep_time").alias("dep_time"),
    col("dep_delay").alias("dep_delay"),
    col("taxi_out").alias("taxi_out"),
    col("taxi_in").alias("taxi_in"),
    col("crs_arr_time").alias("crs_arr_time"),
    col("arr_time").alias("arr_time"),
    col("arr_delay").alias("arr_delay"),
    col("cancelled").alias("cancelled"),
    col("cancellation_code").alias("cancellation_code"),
    col("diverted").alias("diverted"),
    col("crs_elapsed_time").alias("crs_elapsed_time"),
    col("actual_elapsed_time").alias("actual_elapsed_time"),
    col("distance").alias("distance"),
    col("carrier_delay").alias("carrier_delay"),
    col("weather_delay").alias("weather_delay"),
    col("nas_delay").alias("nas_delay"),
    col("security_delay").alias("security_delay"),
    col("late_aircraft_delay").alias("late_aircraft_delay"),
    col("quarter").alias("quarter1"),
    col("flight_status").alias("flight_status"),
    col("ground_time").alias("ground_time"),
    col("month").alias("month_flight")
).distinct()

df_fact_flights.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "FACT_FLIGHTS") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_2015=spark.read.format("parquet").load('dbfs:/project/curated/2015/')
df_2015.columns

# COMMAND ----------

from pyspark.sql.functions import col,when
df_dim_carrier = df_2015.select(
    col("OP_CARRIER").alias("carrier_code"),
    col("Carrier_name").alias("carrier_name")
).distinct()
df_dim_carrier.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_CARRIER_STAGING") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_2015.select(col("OP_CARRIER")).distinct().show(truncate=False)

# COMMAND ----------

df_dim_airport=df_2015.select(
    col("origin").alias("origin_airport_code"),
    col("dest").alias("dest_airport_code")
).distinct()
df_dim_airport.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_STAGING_AIRPORT") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_dim_date=df_2015.select(
    col("fl_date").alias("flight_date"),
    col("quarter").alias("quarter"),
    col("is_weekend").alias("is_weekend")
).distinct()
df_dim_date.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_DATE") \
  .mode("append") \
  .save()

# COMMAND ----------

df_fact_flights=df_2015.select(
    col("fl_date").alias("flight_date"),
    col("op_carrier_fl_num").alias("op_carrier_fl_num"),
    col("op_carrier").alias("carrier_code"),
    col("origin").alias("airport_code"),
    col("dest").alias("airport_code"),
    col("crs_dep_time").alias("crs_dep_time"),
    col("dep_time").alias("dep_time"),
    col("dep_delay").alias("dep_delay"),
    col("taxi_out").alias("taxi_out"),
    col("taxi_in").alias("taxi_in"),
    col("crs_arr_time").alias("crs_arr_time"),
    col("arr_time").alias("arr_time"),
    col("arr_delay").alias("arr_delay"),
    col("cancelled").alias("cancelled"),
    col("cancellation_code").alias("cancellation_code"),
    col("diverted").alias("diverted"),
    col("crs_elapsed_time").alias("crs_elapsed_time"),
    col("actual_elapsed_time").alias("actual_elapsed_time"),
    col("distance").alias("distance"),
    col("carrier_delay").alias("carrier_delay"),
    col("weather_delay").alias("weather_delay"),
    col("nas_delay").alias("nas_delay"),
    col("security_delay").alias("security_delay"),
    col("late_aircraft_delay").alias("late_aircraft_delay"),
    col("quarter").alias("quarter1"),
    col("flight_status").alias("flight_status"),
    col("ground_time").alias("ground_time"),
    col("month").alias("month_flight")
).distinct()
df_fact_flights.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "FACT_FLIGHTS") \
  .mode("append") \
  .save()

# COMMAND ----------

df_2016=spark.read.format("parquet").load('dbfs:/project/curated/2016/')
df_2016.columns

# COMMAND ----------

from pyspark.sql.functions import col,when
df_dim_carrier = df_2016.select(
    col("OP_CARRIER").alias("carrier_code"),
    col("Carrier_name").alias("carrier_name")
).distinct()
df_dim_carrier.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_CARRIER_STAGING") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_2016.select(col("OP_CARRIER")).distinct().show(truncate=False)

# COMMAND ----------

df_dim_airport=df_2016.select(
    col("origin").alias("origin_airport_code"),
    col("dest").alias("dest_airport_code")
).distinct()
df_dim_airport.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_STAGING_AIRPORT") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_dim_date=df_2016.select(
    col("fl_date").alias("flight_date"),
    col("quarter").alias("quarter"),
    col("is_weekend").alias("is_weekend")
).distinct()
df_dim_date.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_DATE") \
  .mode("append") \
  .save()

# COMMAND ----------

df_fact_flights=df_2016.select(
    col("fl_date").alias("flight_date"),
    col("op_carrier_fl_num").alias("op_carrier_fl_num"),
    col("op_carrier").alias("carrier_code"),
    col("origin").alias("airport_code"),
    col("dest").alias("airport_code"),
    col("crs_dep_time").alias("crs_dep_time"),
    col("dep_time").alias("dep_time"),
    col("dep_delay").alias("dep_delay"),
    col("taxi_out").alias("taxi_out"),
    col("taxi_in").alias("taxi_in"),
    col("crs_arr_time").alias("crs_arr_time"),
    col("arr_time").alias("arr_time"),
    col("arr_delay").alias("arr_delay"),
    col("cancelled").alias("cancelled"),
    col("cancellation_code").alias("cancellation_code"),
    col("diverted").alias("diverted"),
    col("crs_elapsed_time").alias("crs_elapsed_time"),
    col("actual_elapsed_time").alias("actual_elapsed_time"),
    col("distance").alias("distance"),
    col("carrier_delay").alias("carrier_delay"),
    col("weather_delay").alias("weather_delay"),
    col("nas_delay").alias("nas_delay"),
    col("security_delay").alias("security_delay"),
    col("late_aircraft_delay").alias("late_aircraft_delay"),
    col("quarter").alias("quarter1"),
    col("flight_status").alias("flight_status"),
    col("ground_time").alias("ground_time"),
    col("month").alias("month_flight")
).distinct()
df_fact_flights.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "FACT_FLIGHTS") \
  .mode("append") \
  .save()

# COMMAND ----------

df_2017=spark.read.format("parquet").load('dbfs:/project/curated/2017/')
df_2017.columns

# COMMAND ----------

from pyspark.sql.functions import col,when
df_dim_carrier = df_2017.select(
    col("OP_CARRIER").alias("carrier_code"),
    col("Carrier_name").alias("carrier_name")
).distinct()
df_dim_carrier.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_CARRIER_STAGING") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_2017.select(col("OP_CARRIER")).distinct().show(truncate=False)

# COMMAND ----------

df_dim_airport=df_2017.select(
    col("origin").alias("origin_airport_code"),
    col("dest").alias("dest_airport_code")
).distinct()
df_dim_airport.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_STAGING_AIRPORT") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_dim_date=df_2017.select(
    col("fl_date").alias("flight_date"),
    col("quarter").alias("quarter"),
    col("is_weekend").alias("is_weekend")
).distinct()
df_dim_date.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_DATE") \
  .mode("append") \
  .save()

# COMMAND ----------

df_fact_flights=df_2017.select(
    col("fl_date").alias("flight_date"),
    col("op_carrier_fl_num").alias("op_carrier_fl_num"),
    col("op_carrier").alias("carrier_code"),
    col("origin").alias("airport_code"),
    col("dest").alias("airport_code"),
    col("crs_dep_time").alias("crs_dep_time"),
    col("dep_time").alias("dep_time"),
    col("dep_delay").alias("dep_delay"),
    col("taxi_out").alias("taxi_out"),
    col("taxi_in").alias("taxi_in"),
    col("crs_arr_time").alias("crs_arr_time"),
    col("arr_time").alias("arr_time"),
    col("arr_delay").alias("arr_delay"),
    col("cancelled").alias("cancelled"),
    col("cancellation_code").alias("cancellation_code"),
    col("diverted").alias("diverted"),
    col("crs_elapsed_time").alias("crs_elapsed_time"),
    col("actual_elapsed_time").alias("actual_elapsed_time"),
    col("distance").alias("distance"),
    col("carrier_delay").alias("carrier_delay"),
    col("weather_delay").alias("weather_delay"),
    col("nas_delay").alias("nas_delay"),
    col("security_delay").alias("security_delay"),
    col("late_aircraft_delay").alias("late_aircraft_delay"),
    col("quarter").alias("quarter1"),
    col("flight_status").alias("flight_status"),
    col("ground_time").alias("ground_time"),
    col("month").alias("month_flight")
).distinct()
df_fact_flights.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "FACT_FLIGHTS") \
  .mode("append") \
  .save()

# COMMAND ----------

df_2018=spark.read.format("parquet").load('dbfs:/project/curated/2018/')
df_2018.columns

# COMMAND ----------

from pyspark.sql.functions import col,when
df_dim_carrier = df_2018.select(
    col("OP_CARRIER").alias("carrier_code"),
    col("Carrier_name").alias("carrier_name")
).distinct()
df_dim_carrier.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_CARRIER_STAGING") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_2018.select("OP_CARRIER").distinct().show(truncate=False)

# COMMAND ----------

df_dim_airport=df_2018.select(
    col("origin").alias("origin_airport_code"),
    col("dest").alias("dest_airport_code")
).distinct()
df_dim_airport.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_STAGING_AIRPORT") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_dim_date=df_2018.select(
    col("fl_date").alias("flight_date"),
    col("quarter").alias("quarter"),
    col("is_weekend").alias("is_weekend")
).distinct()
df_dim_date.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_DATE") \
  .mode("append") \
  .save()

# COMMAND ----------

df_fact_flights=df_2018.select(
    col("fl_date").alias("flight_date"),
    col("op_carrier_fl_num").alias("op_carrier_fl_num"),
    col("op_carrier").alias("carrier_code"),
    col("origin").alias("airport_code"),
    col("dest").alias("airport_code"),
    col("crs_dep_time").alias("crs_dep_time"),
    col("dep_time").alias("dep_time"),
    col("dep_delay").alias("dep_delay"),
    col("taxi_out").alias("taxi_out"),
    col("taxi_in").alias("taxi_in"),
    col("crs_arr_time").alias("crs_arr_time"),
    col("arr_time").alias("arr_time"),
    col("arr_delay").alias("arr_delay"),
    col("cancelled").alias("cancelled"),
    col("cancellation_code").alias("cancellation_code"),
    col("diverted").alias("diverted"),
    col("crs_elapsed_time").alias("crs_elapsed_time"),
    col("actual_elapsed_time").alias("actual_elapsed_time"),
    col("distance").alias("distance"),
    col("carrier_delay").alias("carrier_delay"),
    col("weather_delay").alias("weather_delay"),
    col("nas_delay").alias("nas_delay"),
    col("security_delay").alias("security_delay"),
    col("late_aircraft_delay").alias("late_aircraft_delay"),
    col("quarter").alias("quarter1"),
    col("flight_status").alias("flight_status"),
    col("ground_time").alias("ground_time"),
    col("month").alias("month_flight")
).distinct()
df_fact_flights.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "FACT_FLIGHTS") \
  .mode("append") \
  .save()

# COMMAND ----------

df_2020=spark.read.format("parquet").load('dbfs:/project/curated/2020/')
df_2020.columns

# COMMAND ----------

from pyspark.sql.functions import col,when
df_dim_carrier = df_2020.select(
    col("OP_CARRIER").alias("carrier_code"),
    col("Carrier_name").alias("carrier_name")
).distinct()
df_dim_carrier.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_CARRIER_STAGING") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_2020.select("OP_CARRIER").distinct().show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col,when
df_dim_carrier = df_2020.select(
    col("OP_CARRIER").alias("carrier_code"),
    col("Carrier_name").alias("carrier_name")
).distinct()
df_dim_carrier.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_CARRIER") \
  .mode("append") \
  .save()

# COMMAND ----------

df_dim_airport=df_2020.select(
    col("origin").alias("origin_airport_code"),
    col("dest").alias("dest_airport_code")
).distinct()
df_dim_airport.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_STAGING_AIRPORT") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

df_dim_date=df_2020.select(
    col("fl_date").alias("flight_date"),
    col("quarter").alias("quarter"),
    col("is_weekend").alias("is_weekend")
).distinct()
df_dim_date.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "DIM_DATE") \
  .mode("append") \
  .save()

# COMMAND ----------

df_fact_flights=df_2020.select(
    col("fl_date").alias("flight_date"),
    col("op_carrier_fl_num").alias("op_carrier_fl_num"),
    col("op_carrier").alias("carrier_code"),
    col("origin").alias("airport_code"),
    col("dest").alias("airport_code"),
    col("crs_dep_time").alias("crs_dep_time"),
    col("dep_time").alias("dep_time"),
    col("dep_delay").alias("dep_delay"),
    col("taxi_out").alias("taxi_out"),
    col("taxi_in").alias("taxi_in"),
    col("crs_arr_time").alias("crs_arr_time"),
    col("arr_time").alias("arr_time"),
    col("arr_delay").alias("arr_delay"),
    col("cancelled").alias("cancelled"),
    col("cancellation_code").alias("cancellation_code"),
    col("diverted").alias("diverted"),
    col("crs_elapsed_time").alias("crs_elapsed_time"),
    col("actual_elapsed_time").alias("actual_elapsed_time"),
    col("distance").alias("distance"),
    col("carrier_delay").alias("carrier_delay"),
    col("weather_delay").alias("weather_delay"),
    col("nas_delay").alias("nas_delay"),
    col("security_delay").alias("security_delay"),
    col("late_aircraft_delay").alias("late_aircraft_delay"),
    col("quarter").alias("quarter1"),
    col("flight_status").alias("flight_status"),
    col("ground_time").alias("ground_time"),
    col("month").alias("month_flight")
).distinct()
df_fact_flights.write \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "FACT_FLIGHTS") \
  .mode("append") \
  .save()

# COMMAND ----------

