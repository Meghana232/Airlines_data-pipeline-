# Databricks notebook source
access_key = dbutils.secrets.get(scope="s3-creds", key="aws-access-key")
secret_key = dbutils.secrets.get(scope="s3-creds", key="aws-secret-key")

spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

# COMMAND ----------

df_2014=spark.read.format("parquet").load("dbfs:/project/processed/2014/")

# COMMAND ----------

from pyspark.sql.functions import when, col

df_2014 = df_2014.withColumn(
    "Flight_Status",
    when(col("CANCELLED") == 1, "Cancelled")
    .when(col("DIVERTED") == 1, "Diverted")
    .when(col("DEP_DELAY_COMPUTED") > 0, "Delayed")
    .otherwise("On Time")
)\
    .withColumn("Ground_Time",
                col("taxi_in")+col("taxi_out"))


# COMMAND ----------

from pyspark.sql.functions import col, to_date, dayofweek
df_2014 = df_2014.withColumn("is_weekend", dayofweek(to_date(col("FL_DATE"))).isin([1, 7]))

# COMMAND ----------

df_2014=df_2014.withColumn("Carrier_name",
                           when(col("OP_CARRIER") == "UA", "United Airlines")
                           .when(col("OP_CARRIER") == "DL", "Delta Air Lines")
                           .when(col("OP_CARRIER") == "AA", "American Airlines")
                           .when(col("OP_CARRIER") == "B6", "JetBlue Airways")
                           .when(col("OP_CARRIER") == "WN", "Southwest Airlines")
                           .when(col("OP_CARRIER") == "EV", "ExpressJet Airlines")
                           .when(col("OP_CARRIER") == "F9", "Frontier Airlines")
                           .when(col("OP_CARRIER") == "NK", "Spirit Airlines")
                           .when(col("OP_CARRIER") == "HA", "Hawaiian Airlines")
                           .when(col("OP_CARRIER") == "AS", "Alaska Airlines")
                           .when(col("OP_CARRIER") == "OO", "SkyWest Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "YV", "Mesa Airlines")
                           .when(col("OP_CARRIER") == "CO", "Continental Airlines")
                           .when(col("OP_CARRIER") == "FL", "AIR TRAN Airlines")
                           .when(col("OP_CARRIER") == "OH", "PSA Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "XE", "Express Jet Airlines")
                           .when(col("OP_CARRIER") == "9E", "Endeavor Air")
                           .when(col("OP_CARRIER") == "MQ", "Envoy Air")
                           .when(col("OP_CARRIER") == "VX", "Virgin America"))


# COMMAND ----------

df_2014.columns

# COMMAND ----------

df_2014.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("s3a://endtoenddataengineerproject/curated/2014")
df_2014.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("dbfs:/project/curated/2014")

# COMMAND ----------

df_2015=spark.read.format("parquet").load("dbfs:/project/processed/2015/")

# COMMAND ----------

from pyspark.sql.functions import when, col

df_2015 = df_2015.withColumn(
    "Flight_Status",
    when(col("CANCELLED") == 1, "Cancelled")
    .when(col("DIVERTED") == 1, "Diverted")
    .when(col("DEP_DELAY_COMPUTED") > 0, "Delayed")
    .otherwise("On Time")
)\
    .withColumn("Ground_Time",
                col("taxi_in")+col("taxi_out"))


# COMMAND ----------

df_2015=df_2015.withColumn("Carrier_name",
                           when(col("OP_CARRIER") == "UA", "United Airlines")
                           .when(col("OP_CARRIER") == "DL", "Delta Air Lines")
                           .when(col("OP_CARRIER") == "AA", "American Airlines")
                           .when(col("OP_CARRIER") == "B6", "JetBlue Airways")
                           .when(col("OP_CARRIER") == "WN", "Southwest Airlines")
                           .when(col("OP_CARRIER") == "EV", "ExpressJet Airlines")
                           .when(col("OP_CARRIER") == "F9", "Frontier Airlines")
                           .when(col("OP_CARRIER") == "NK", "Spirit Airlines")
                           .when(col("OP_CARRIER") == "HA", "Hawaiian Airlines")
                           .when(col("OP_CARRIER") == "AS", "Alaska Airlines")
                           .when(col("OP_CARRIER") == "OO", "SkyWest Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "YV", "Mesa Airlines")
                           .when(col("OP_CARRIER") == "CO", "Continental Airlines")
                           .when(col("OP_CARRIER") == "FL", "AIR TRAN Airlines")
                           .when(col("OP_CARRIER") == "OH", "PSA Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "XE", "Express Jet Airlines")
                           .when(col("OP_CARRIER") == "9E", "Endeavor Air")
                           .when(col("OP_CARRIER") == "MQ", "Envoy Air")
                           .when(col("OP_CARRIER") == "VX", "Virgin America"))


# COMMAND ----------

from pyspark.sql.functions import col, to_date, dayofweek
df_2015 = df_2015.withColumn("is_weekend", dayofweek(to_date(col("FL_DATE"))).isin([1, 7]))

# COMMAND ----------

df_2015.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("s3a://endtoenddataengineerproject/curated/2015")
df_2015.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("dbfs:/project/curated/2015")

# COMMAND ----------

df_2016=spark.read.format("parquet").load("dbfs:/project/processed/2016/")

# COMMAND ----------

from pyspark.sql.functions import when, col

df_2016 = df_2016.withColumn(
    "Flight_Status",
    when(col("CANCELLED") == 1, "Cancelled")
    .when(col("DIVERTED") == 1, "Diverted")
    .when(col("DEP_DELAY_COMPUTED") > 0, "Delayed")
    .otherwise("On Time")
)\
    .withColumn("Ground_Time",
                col("taxi_in")+col("taxi_out"))


# COMMAND ----------

df_2016=df_2016.withColumn("Carrier_name",
                           when(col("OP_CARRIER") == "UA", "United Airlines")
                           .when(col("OP_CARRIER") == "DL", "Delta Air Lines")
                           .when(col("OP_CARRIER") == "AA", "American Airlines")
                           .when(col("OP_CARRIER") == "B6", "JetBlue Airways")
                           .when(col("OP_CARRIER") == "WN", "Southwest Airlines")
                           .when(col("OP_CARRIER") == "EV", "ExpressJet Airlines")
                           .when(col("OP_CARRIER") == "F9", "Frontier Airlines")
                           .when(col("OP_CARRIER") == "NK", "Spirit Airlines")
                           .when(col("OP_CARRIER") == "HA", "Hawaiian Airlines")
                           .when(col("OP_CARRIER") == "AS", "Alaska Airlines")
                           .when(col("OP_CARRIER") == "OO", "SkyWest Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "YV", "Mesa Airlines")
                           .when(col("OP_CARRIER") == "CO", "Continental Airlines")
                           .when(col("OP_CARRIER") == "FL", "AIR TRAN Airlines")
                           .when(col("OP_CARRIER") == "OH", "PSA Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "XE", "Express Jet Airlines")
                           .when(col("OP_CARRIER") == "9E", "Endeavor Air")
                           .when(col("OP_CARRIER") == "MQ", "Envoy Air")
                           .when(col("OP_CARRIER") == "VX", "Virgin America"))


# COMMAND ----------

from pyspark.sql.functions import col, to_date, dayofweek
df_2016 = df_2016.withColumn("is_weekend", dayofweek(to_date(col("FL_DATE"))).isin([1, 7]))

# COMMAND ----------

df_2016.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("s3a://endtoenddataengineerproject/curated/2016")
df_2016.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("dbfs:/project/curated/2016")

# COMMAND ----------

df_2017=spark.read.format("parquet").load("dbfs:/project/processed/2017/")

# COMMAND ----------

from pyspark.sql.functions import when, col

df_2017 = df_2017.withColumn(
    "Flight_Status",
    when(col("CANCELLED") == 1, "Cancelled")
    .when(col("DIVERTED") == 1, "Diverted")
    .when(col("DEP_DELAY_COMPUTED") > 0, "Delayed")
    .otherwise("On Time")
)\
    .withColumn("Ground_Time",
                col("taxi_in")+col("taxi_out"))


# COMMAND ----------

df_2017=df_2017.withColumn("Carrier_name",
                           when(col("OP_CARRIER") == "UA", "United Airlines")
                           .when(col("OP_CARRIER") == "DL", "Delta Air Lines")
                           .when(col("OP_CARRIER") == "AA", "American Airlines")
                           .when(col("OP_CARRIER") == "B6", "JetBlue Airways")
                           .when(col("OP_CARRIER") == "WN", "Southwest Airlines")
                           .when(col("OP_CARRIER") == "EV", "ExpressJet Airlines")
                           .when(col("OP_CARRIER") == "F9", "Frontier Airlines")
                           .when(col("OP_CARRIER") == "NK", "Spirit Airlines")
                           .when(col("OP_CARRIER") == "HA", "Hawaiian Airlines")
                           .when(col("OP_CARRIER") == "AS", "Alaska Airlines")
                           .when(col("OP_CARRIER") == "OO", "SkyWest Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "YV", "Mesa Airlines")
                           .when(col("OP_CARRIER") == "CO", "Continental Airlines")
                           .when(col("OP_CARRIER") == "FL", "AIR TRAN Airlines")
                           .when(col("OP_CARRIER") == "OH", "PSA Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "XE", "Express Jet Airlines")
                           .when(col("OP_CARRIER") == "9E", "Endeavor Air")
                           .when(col("OP_CARRIER") == "MQ", "Envoy Air")
                           .when(col("OP_CARRIER") == "VX", "Virgin America"))


# COMMAND ----------

from pyspark.sql.functions import col, to_date, dayofweek
df_2017 = df_2017.withColumn("is_weekend", dayofweek(to_date(col("FL_DATE"))).isin([1, 7]))

# COMMAND ----------

df_2017.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("s3a://endtoenddataengineerproject/curated/2017")
df_2017.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("dbfs:/project/curated/2017")

# COMMAND ----------

df_2018=spark.read.format("parquet").load("dbfs:/project/processed/2018/")

# COMMAND ----------

from pyspark.sql.functions import when, col

df_2018 = df_2018.withColumn(
    "Flight_Status",
    when(col("CANCELLED") == 1, "Cancelled")
    .when(col("DIVERTED") == 1, "Diverted")
    .when(col("DEP_DELAY_COMPUTED") > 0, "Delayed")
    .otherwise("On Time")
)\
    .withColumn("Ground_Time",
                col("taxi_in")+col("taxi_out"))


# COMMAND ----------

df_2018=df_2018.withColumn("Carrier_name",
                           when(col("OP_CARRIER") == "UA", "United Airlines")
                           .when(col("OP_CARRIER") == "DL", "Delta Air Lines")
                           .when(col("OP_CARRIER") == "AA", "American Airlines")
                           .when(col("OP_CARRIER") == "B6", "JetBlue Airways")
                           .when(col("OP_CARRIER") == "WN", "Southwest Airlines")
                           .when(col("OP_CARRIER") == "EV", "ExpressJet Airlines")
                           .when(col("OP_CARRIER") == "F9", "Frontier Airlines")
                           .when(col("OP_CARRIER") == "NK", "Spirit Airlines")
                           .when(col("OP_CARRIER") == "HA", "Hawaiian Airlines")
                           .when(col("OP_CARRIER") == "AS", "Alaska Airlines")
                           .when(col("OP_CARRIER") == "OO", "SkyWest Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "YV", "Mesa Airlines")
                           .when(col("OP_CARRIER") == "CO", "Continental Airlines")
                           .when(col("OP_CARRIER") == "FL", "AIR TRAN Airlines")
                           .when(col("OP_CARRIER") == "OH", "PSA Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "XE", "Express Jet Airlines")
                           .when(col("OP_CARRIER") == "9E", "Endeavor Air")
                           .when(col("OP_CARRIER") == "MQ", "Envoy Air")
                           .when(col("OP_CARRIER") == "VX", "Virgin America"))


# COMMAND ----------

from pyspark.sql.functions import col, to_date, dayofweek
df_2018 = df_2018.withColumn("is_weekend", dayofweek(to_date(col("FL_DATE"))).isin([1, 7]))

# COMMAND ----------

df_2018.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("s3a://endtoenddataengineerproject/curated/2018")
df_2018.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("dbfs:/project/curated/2018")

# COMMAND ----------

df_2020=spark.read.format("parquet").load("dbfs:/project/processed/2020/")

# COMMAND ----------

from pyspark.sql.functions import when, col

df_2020 = df_2020.withColumn(
    "Flight_Status",
    when(col("CANCELLED") == 1, "Cancelled")
    .when(col("DIVERTED") == 1, "Diverted")
    .when(col("DEP_DELAY_COMPUTED") > 0, "Delayed")
    .otherwise("On Time")
)\
    .withColumn("Ground_Time",
                col("taxi_in")+col("taxi_out"))


# COMMAND ----------

df_2020=df_2020.withColumn("Carrier_name",
                           when(col("OP_CARRIER") == "UA", "United Airlines")
                           .when(col("OP_CARRIER") == "DL", "Delta Air Lines")
                           .when(col("OP_CARRIER") == "AA", "American Airlines")
                           .when(col("OP_CARRIER") == "B6", "JetBlue Airways")
                           .when(col("OP_CARRIER") == "WN", "Southwest Airlines")
                           .when(col("OP_CARRIER") == "EV", "ExpressJet Airlines")
                           .when(col("OP_CARRIER") == "F9", "Frontier Airlines")
                           .when(col("OP_CARRIER") == "NK", "Spirit Airlines")
                           .when(col("OP_CARRIER") == "HA", "Hawaiian Airlines")
                           .when(col("OP_CARRIER") == "AS", "Alaska Airlines")
                           .when(col("OP_CARRIER") == "OO", "SkyWest Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "YV", "Mesa Airlines")
                           .when(col("OP_CARRIER") == "CO", "Continental Airlines")
                           .when(col("OP_CARRIER") == "FL", "AIR TRAN Airlines")
                           .when(col("OP_CARRIER") == "OH", "PSA Airlines")
                           .when(col("OP_CARRIER") == "US", "US Airways")
                           .when(col("OP_CARRIER") == "XE", "Express Jet Airlines")
                           .when(col("OP_CARRIER") == "9E", "Endeavor Air")
                           .when(col("OP_CARRIER") == "MQ", "Envoy Air")
                           .when(col("OP_CARRIER") == "VX", "Virgin America"))


# COMMAND ----------

from pyspark.sql.functions import col, to_date, dayofweek
df_2020 = df_2020.withColumn("is_weekend", dayofweek(to_date(col("FL_DATE"))).isin([1, 7]))

# COMMAND ----------

df_2020.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("s3a://endtoenddataengineerproject/curated/2020")
df_2020.write.partitionBy("month","OP_CARRIER").mode("overwrite").parquet("dbfs:/project/curated/2020")

# COMMAND ----------

