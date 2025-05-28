# Databricks notebook source
access_key = dbutils.secrets.get(scope="s3-creds", key="aws-access-key")
secret_key = dbutils.secrets.get(scope="s3-creds", key="aws-secret-key")

spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

# COMMAND ----------

dbutils.fs.ls("dbfs:/project/raw")

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/project/raw/2014.csv")  # Sample dataset
df_pd = df.toPandas()  # Convert Spark DataFrame to Pandas if using boto3
local_path = "/tmp/2014.csv"
df_pd.to_csv(local_path, header=True,index=False) 

# COMMAND ----------

import boto3
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
s3.upload_file(local_path, "endtoenddataengineerproject", "raw/2014.csv")

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/project/raw/2015.csv")  # Sample dataset
df_pd = df.toPandas()  # Convert Spark DataFrame to Pandas if using boto3
local_path = "/tmp/2015.csv"
df_pd.to_csv(local_path, header=True,index=False) 

# COMMAND ----------

import boto3
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
s3.upload_file(local_path, "endtoenddataengineerproject", "raw/2015.csv")

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/project/raw/2016.csv")  # Sample dataset
df_pd = df.toPandas()  # Convert Spark DataFrame to Pandas if using boto3
local_path = "/tmp/2016.csv"
df_pd.to_csv(local_path, header=True,index=False) 

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/project/raw/2017.csv")  # Sample dataset
df_pd = df.toPandas()  # Convert Spark DataFrame to Pandas if using boto3
local_path = "/tmp/2017.csv"
df_pd.to_csv(local_path, header=True,index=False) 

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/project/raw/2018.csv")  # Sample dataset
df_pd = df.toPandas()  # Convert Spark DataFrame to Pandas if using boto3
local_path = "/tmp/2018.csv"
df_pd.to_csv(local_path, header=True,index=False) 

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/project/raw/2020.csv")  # Sample dataset
df_pd = df.toPandas()  # Convert Spark DataFrame to Pandas if using boto3
local_path = "/tmp/2020.csv"
df_pd.to_csv(local_path, header=True,index=False) 

# COMMAND ----------

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
s3.upload_file(local_path, "endtoenddataengineerproject", "raw/2016.csv")
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
s3.upload_file(local_path, "endtoenddataengineerproject", "raw/2017.csv")
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
s3.upload_file(local_path, "endtoenddataengineerproject", "raw/2018.csv")
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
s3.upload_file(local_path, "endtoenddataengineerproject", "raw/2020.csv")

# COMMAND ----------

