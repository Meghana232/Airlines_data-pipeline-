# Databricks notebook source
access_key = dbutils.secrets.get(scope="s3-creds", key="aws-access-key")
secret_key = dbutils.secrets.get(scope="s3-creds", key="aws-secret-key")

spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
spark.conf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

# COMMAND ----------

df_201=spark.read.csv('dbfs:/project/raw/2014.csv',header=True,inferSchema=True)
df_201.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df_201=df_201.withColumn("CRS_DEP_TIME",col("CRS_DEP_TIME").cast("string"))\
    .withColumn("DEP_TIME",col("DEP_TIME").cast("string"))\
        .withColumn("CRS_ARR_TIME",col("CRS_ARR_TIME").cast("string"))\
            .withColumn("ARR_TIME",col("ARR_TIME").cast("string"))
df_201.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,lpad,concat_ws
df_201=df_201.withColumn("CRS_DEP_TIME",lpad(col("CRS_DEP_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_DEP_TIME",concat_ws(":",col("CRS_DEP_TIME").substr(1,2),col("CRS_DEP_TIME").substr(3,2)))\
    .withColumn("DEP_TIME",lpad(col("DEP_TIME").cast("string"),4,"0"))\
    .withColumn("DEP_TIME",concat_ws(":",col("DEP_TIME").substr(1,2),col("DEP_TIME").substr(3,2)))\
    .withColumn("CRS_ARR_TIME",lpad(col("CRS_ARR_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_ARR_TIME",concat_ws(":",col("CRS_ARR_TIME").substr(1,2),col("CRS_ARR_TIME").substr(3,2)))\
    .withColumn("ARR_TIME",lpad(col("ARR_TIME").cast("string"),4,"0"))\
        .withColumn("ARR_TIME",concat_ws(":",col("ARR_TIME").substr(1,2),col("ARR_TIME").substr(3,2)))

# COMMAND ----------

from pyspark.sql.functions import upper
df_201.select(upper("origin").alias("origin"),
               upper("dest").alias("dest")).show()

# COMMAND ----------

df_201.count()

# COMMAND ----------

df_201.where(col("CRS_DEP_TIME").isNotNull()).count()

# COMMAND ----------

df_201.where(col("FL_DATE").isNotNull()).count()

# COMMAND ----------

df_201.where(col("origin").isNotNull()).count()

# COMMAND ----------

df_201.where(col("dest").isNotNull()).count()

# COMMAND ----------

from pyspark.sql.functions import quarter,concat,lit
df_201=df_201.withColumn("quarter",concat(lit("Q"), quarter("FL_DATE"))) 

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat_ws, col, lit

# Convert back to full datetime for comparison
df_201 = df_201 \
    .withColumn("CRS_DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("CRS_ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_ARR_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("ARR_TIME")), "yyyy-MM-dd HH:mm"))

# COMMAND ----------

from pyspark.sql.functions import (col, expr)

df_201 = df_201 \
    .withColumn("DEP_DELAY_COMPUTED", (col("DEP_DATETIME").cast("long") - col("CRS_DEP_DATETIME").cast("long")) / 60) \
    .withColumn("ARR_DELAY_COMPUTED", (col("ARR_DATETIME").cast("long") - col("CRS_ARR_DATETIME").cast("long")) / 60)


# COMMAND ----------

df_201_inconsistent_record=df_201.select(
    "FL_DATE", "ORIGIN", "DEST",
    "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "DEP_DELAY_COMPUTED",
    "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "ARR_DELAY_COMPUTED"
).where((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))


# COMMAND ----------

df_201_inconsistent_record.write.mode("overwrite").parquet("s3a://endtoenddataengineerproject/inconsistent_records/2014/")
df_201_inconsistent_record.write.mode("overwrite").parquet("dbfs:/project/incosistent_records/2014/")


# COMMAND ----------

#finding inconsistent records store these records in a separate data frame and upload it in aws s3 path.
df_2014_inconsistent_records = df_201.filter(
    ~((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))
)

# COMMAND ----------

#remove these records from the dataset i.e., delete it 
from pyspark.sql.functions import col

df_inconsistent = df_2014_inconsistent_records.filter(~((col("CANCELLED") == 1) & (col("DEP_TIME").isNotNull())))

# COMMAND ----------

from pyspark.sql.functions import month,col
df_inconsistent=df_inconsistent.withColumn("month",month(col("FL_DATE")))

# COMMAND ----------

df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("s3a://endtoenddataengineerproject/processed/2014/")
df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("dbfs:/project/processed/2014/")

# COMMAND ----------

df_2015=spark.read.csv('dbfs:/project/raw/2015.csv',header=True,inferSchema=True)
df_2015.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df_2015=df_2015.withColumn("CRS_DEP_TIME",col("CRS_DEP_TIME").cast("string"))\
    .withColumn("DEP_TIME",col("DEP_TIME").cast("string"))\
        .withColumn("CRS_ARR_TIME",col("CRS_ARR_TIME").cast("string"))\
            .withColumn("ARR_TIME",col("ARR_TIME").cast("string"))
df_2015.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,lpad,concat_ws
df_2015=df_2015.withColumn("CRS_DEP_TIME",lpad(col("CRS_DEP_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_DEP_TIME",concat_ws(":",col("CRS_DEP_TIME").substr(1,2),col("CRS_DEP_TIME").substr(3,2)))\
    .withColumn("DEP_TIME",lpad(col("DEP_TIME").cast("string"),4,"0"))\
    .withColumn("DEP_TIME",concat_ws(":",col("DEP_TIME").substr(1,2),col("DEP_TIME").substr(3,2)))\
    .withColumn("CRS_ARR_TIME",lpad(col("CRS_ARR_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_ARR_TIME",concat_ws(":",col("CRS_ARR_TIME").substr(1,2),col("CRS_ARR_TIME").substr(3,2)))\
    .withColumn("ARR_TIME",lpad(col("ARR_TIME").cast("string"),4,"0"))\
        .withColumn("ARR_TIME",concat_ws(":",col("ARR_TIME").substr(1,2),col("ARR_TIME").substr(3,2)))

# COMMAND ----------

from pyspark.sql.functions import upper
df_2015.select(upper("origin").alias("origin"),
               upper("dest").alias("dest")).show()

# COMMAND ----------

df_2015.count()

# COMMAND ----------

df_2015.where(col("CRS_DEP_TIME").isNotNull()).count()

# COMMAND ----------

df_2015.where(col("FL_DATE").isNotNull()).count()

# COMMAND ----------

df_2015.where(col("origin").isNotNull()).count()

# COMMAND ----------

df_2015.where(col("dest").isNotNull()).count()

# COMMAND ----------

from pyspark.sql.functions import quarter,concat,lit
df_2015=df_2015.withColumn("quarter",concat(lit("Q"), quarter("FL_DATE"))) 

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat_ws, col, lit

# Convert back to full datetime for comparison
df_2015 = df_2015 \
    .withColumn("CRS_DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("CRS_ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_ARR_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("ARR_TIME")), "yyyy-MM-dd HH:mm"))

# COMMAND ----------

from pyspark.sql.functions import (col, expr)

df_2015 = df_2015 \
    .withColumn("DEP_DELAY_COMPUTED", (col("DEP_DATETIME").cast("long") - col("CRS_DEP_DATETIME").cast("long")) / 60) \
    .withColumn("ARR_DELAY_COMPUTED", (col("ARR_DATETIME").cast("long") - col("CRS_ARR_DATETIME").cast("long")) / 60)


# COMMAND ----------

df_2015_inconsistent_record=df_2015.select(
    "FL_DATE", "ORIGIN", "DEST",
    "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "DEP_DELAY_COMPUTED",
    "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "ARR_DELAY_COMPUTED"
).where((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))

# COMMAND ----------

df_2015_inconsistent_record.write.mode("overwrite").parquet("s3a://endtoenddataengineerproject/inconsistent_records/2015/")
df_2015_inconsistent_record.write.mode("overwrite").parquet("dbfs:/endtoenddataengineerproject/incosistent_records/2015/")

# COMMAND ----------

#finding inconsistent records store these records in a separate data frame and upload it in aws s3 path.
df_2015_inconsistent_records =df_2015.filter(
    ~((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))
)

# COMMAND ----------

#remove these records from the dataset i.e., delete it 
from pyspark.sql.functions import col

df_inconsistent = df_2015_inconsistent_records.filter(~((col("CANCELLED") == 1) & (col("DEP_TIME").isNotNull())))

# COMMAND ----------

from pyspark.sql.functions import col, month
df_inconsistent = df_inconsistent.withColumn("month", month("FL_DATE"))

# COMMAND ----------

df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("s3a://endtoenddataengineerproject/processed/2015/")
df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("dbfs:/project/processed/2015/")


# COMMAND ----------

df_2016=spark.read.csv('dbfs:/project/raw/2016.csv',header=True,inferSchema=True)
df_2016.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df_2016=df_2016.withColumn("CRS_DEP_TIME",col("CRS_DEP_TIME").cast("string"))\
    .withColumn("DEP_TIME",col("DEP_TIME").cast("string"))\
        .withColumn("CRS_ARR_TIME",col("CRS_ARR_TIME").cast("string"))\
            .withColumn("ARR_TIME",col("ARR_TIME").cast("string"))
df_2016.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,lpad,concat_ws
df_2016=df_2016.withColumn("CRS_DEP_TIME",lpad(col("CRS_DEP_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_DEP_TIME",concat_ws(":",col("CRS_DEP_TIME").substr(1,2),col("CRS_DEP_TIME").substr(3,2)))\
    .withColumn("DEP_TIME",lpad(col("DEP_TIME").cast("string"),4,"0"))\
    .withColumn("DEP_TIME",concat_ws(":",col("DEP_TIME").substr(1,2),col("DEP_TIME").substr(3,2)))\
    .withColumn("CRS_ARR_TIME",lpad(col("CRS_ARR_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_ARR_TIME",concat_ws(":",col("CRS_ARR_TIME").substr(1,2),col("CRS_ARR_TIME").substr(3,2)))\
    .withColumn("ARR_TIME",lpad(col("ARR_TIME").cast("string"),4,"0"))\
        .withColumn("ARR_TIME",concat_ws(":",col("ARR_TIME").substr(1,2),col("ARR_TIME").substr(3,2)))

# COMMAND ----------

from pyspark.sql.functions import upper
df_2016.select(upper("origin").alias("origin"),
               upper("dest").alias("dest")).show()

# COMMAND ----------

df_2016.count()

# COMMAND ----------

df_2016.where(col("CRS_DEP_TIME").isNotNull()).count()

# COMMAND ----------

df_2016.where(col("FL_DATE").isNotNull()).count()


# COMMAND ----------

df_2016.where(col("origin").isNotNull()).count()


# COMMAND ----------

df_2016.where(col("dest").isNotNull()).count()


# COMMAND ----------

from pyspark.sql.functions import quarter,concat,lit
df_2016=df_2016.withColumn("quarter",concat(lit("Q"), quarter("FL_DATE"))) 


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat_ws, col, lit

# Convert back to full datetime for comparison
df_2016 = df_2016 \
    .withColumn("CRS_DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("CRS_ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_ARR_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("ARR_TIME")), "yyyy-MM-dd HH:mm"))


# COMMAND ----------

from pyspark.sql.functions import (col, expr)

df_2016 = df_2016 \
    .withColumn("DEP_DELAY_COMPUTED", (col("DEP_DATETIME").cast("long") - col("CRS_DEP_DATETIME").cast("long")) / 60) \
    .withColumn("ARR_DELAY_COMPUTED", (col("ARR_DATETIME").cast("long") - col("CRS_ARR_DATETIME").cast("long")) / 60)




# COMMAND ----------

df_2016_inconsistent_record=df_2016.select(
    "FL_DATE", "ORIGIN", "DEST",
    "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "DEP_DELAY_COMPUTED",
    "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "ARR_DELAY_COMPUTED"
).where((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))


# COMMAND ----------

df_2016_inconsistent_record.write.mode("overwrite").parquet("s3a://endtoenddataengineerproject/inconsistent_records/2016/")
df_2016_inconsistent_record.write.mode("overwrite").parquet("dbfs:/project/incosistent_records/2016/")

# COMMAND ----------

#finding inconsistent records store these records in a separate data frame and upload it in aws s3 path.
df_2016_inconsistent_records=df_2016.filter(
    ~((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))
)


# COMMAND ----------

#remove these records from the dataset i.e., delete it 
from pyspark.sql.functions import col

df_inconsistent = df_2016_inconsistent_records.filter(~((col("CANCELLED") == 1) & (col("DEP_TIME").isNotNull())))

# COMMAND ----------

from pyspark.sql.functions import month
df_inconsistent=df_inconsistent.withColumn("month",month("FL_DATE"))

# COMMAND ----------

df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("s3://endtoenddataengineerproject/processed/2016/")
df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("dbfs:/project/processed/2016/")


# COMMAND ----------

df_2017=spark.read.csv('dbfs:/project/raw/2017.csv',header=True,inferSchema=True)
df_2017.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col
df_2017=df_2017.withColumn("CRS_DEP_TIME",col("CRS_DEP_TIME").cast("string"))\
    .withColumn("DEP_TIME",col("DEP_TIME").cast("string"))\
        .withColumn("CRS_ARR_TIME",col("CRS_ARR_TIME").cast("string"))\
            .withColumn("ARR_TIME",col("ARR_TIME").cast("string"))
df_2017.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col,lpad,concat_ws
df_2017=df_2017.withColumn("CRS_DEP_TIME",lpad(col("CRS_DEP_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_DEP_TIME",concat_ws(":",col("CRS_DEP_TIME").substr(1,2),col("CRS_DEP_TIME").substr(3,2)))\
    .withColumn("DEP_TIME",lpad(col("DEP_TIME").cast("string"),4,"0"))\
    .withColumn("DEP_TIME",concat_ws(":",col("DEP_TIME").substr(1,2),col("DEP_TIME").substr(3,2)))\
    .withColumn("CRS_ARR_TIME",lpad(col("CRS_ARR_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_ARR_TIME",concat_ws(":",col("CRS_ARR_TIME").substr(1,2),col("CRS_ARR_TIME").substr(3,2)))\
    .withColumn("ARR_TIME",lpad(col("ARR_TIME").cast("string"),4,"0"))\
        .withColumn("ARR_TIME",concat_ws(":",col("ARR_TIME").substr(1,2),col("ARR_TIME").substr(3,2)))


# COMMAND ----------

from pyspark.sql.functions import upper
df_2017.select(upper("origin").alias("origin"),
               upper("dest").alias("dest")).show()


# COMMAND ----------

df_2017.count()


# COMMAND ----------

df_2017.where(col("CRS_DEP_TIME").isNotNull()).count()


# COMMAND ----------

df_2017.where(col("FL_DATE").isNotNull()).count()


# COMMAND ----------

df_2017.where(col("origin").isNotNull()).count()


# COMMAND ----------

df_2017.where(col("dest").isNotNull()).count()


# COMMAND ----------

from pyspark.sql.functions import quarter,concat,lit
df_2017=df_2017.withColumn("quarter",concat(lit("Q"), quarter("FL_DATE"))) 


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat_ws, col, lit

# Convert back to full datetime for comparison
df_2017 = df_2017 \
    .withColumn("CRS_DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("CRS_ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_ARR_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("ARR_TIME")), "yyyy-MM-dd HH:mm"))


# COMMAND ----------

from pyspark.sql.functions import (col, expr)

df_2017 = df_2017 \
    .withColumn("DEP_DELAY_COMPUTED", (col("DEP_DATETIME").cast("long") - col("CRS_DEP_DATETIME").cast("long")) / 60) \
    .withColumn("ARR_DELAY_COMPUTED", (col("ARR_DATETIME").cast("long") - col("CRS_ARR_DATETIME").cast("long")) / 60)


# COMMAND ----------

#finding inconsistent records store these records in a separate data frame and upload it in aws s3 path.
df_2017_inconsistent_record=df_2017.select(
    "FL_DATE", "ORIGIN", "DEST",
    "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "DEP_DELAY_COMPUTED",
    "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "ARR_DELAY_COMPUTED"
).where((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))



# COMMAND ----------

df_2017_inconsistent_record.write.mode("overwrite").parquet("s3://endtoenddataengineerproject/inconsistent_records/2017/")
df_2017_inconsistent_record.write.mode("overwrite").parquet("dbfs:/project/incosistent_records/2017/")

# COMMAND ----------

#finding inconsistent records store these records in a separate data frame and upload it in aws s3 path.
df_2017_inconsistent_records= df_2017.filter(
    ~((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))
)

# COMMAND ----------

#remove these records from the dataset i.e., delete it 
from pyspark.sql.functions import col

df_inconsistent = df_2017_inconsistent_records.filter((col("CANCELLED") == 1) & (col("DEP_TIME").isNotNull()))

# COMMAND ----------

from pyspark.sql.functions import month
df_inconsistent=df_inconsistent.withColumn("month",month("FL_DATE"))

# COMMAND ----------

df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("s3a://endtoenddataengineerproject/processed/2017/")
df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("dbfs:/project/processed/2017/")

# COMMAND ----------

df_2018=spark.read.csv('dbfs:/project/raw/2018.csv',header=True,inferSchema=True)
df_2018.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col
df_2018=df_2018.withColumn("CRS_DEP_TIME",col("CRS_DEP_TIME").cast("string"))\
    .withColumn("DEP_TIME",col("DEP_TIME").cast("string"))\
        .withColumn("CRS_ARR_TIME",col("CRS_ARR_TIME").cast("string"))\
            .withColumn("ARR_TIME",col("ARR_TIME").cast("string"))
df_2018.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col,lpad,concat_ws
df_2018=df_2018.withColumn("CRS_DEP_TIME",lpad(col("CRS_DEP_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_DEP_TIME",concat_ws(":",col("CRS_DEP_TIME").substr(1,2),col("CRS_DEP_TIME").substr(3,2)))\
    .withColumn("DEP_TIME",lpad(col("DEP_TIME").cast("string"),4,"0"))\
    .withColumn("DEP_TIME",concat_ws(":",col("DEP_TIME").substr(1,2),col("DEP_TIME").substr(3,2)))\
    .withColumn("CRS_ARR_TIME",lpad(col("CRS_ARR_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_ARR_TIME",concat_ws(":",col("CRS_ARR_TIME").substr(1,2),col("CRS_ARR_TIME").substr(3,2)))\
    .withColumn("ARR_TIME",lpad(col("ARR_TIME").cast("string"),4,"0"))\
        .withColumn("ARR_TIME",concat_ws(":",col("ARR_TIME").substr(1,2),col("ARR_TIME").substr(3,2)))


# COMMAND ----------

from pyspark.sql.functions import upper
df_2018.select(upper("origin").alias("origin"),
               upper("dest").alias("dest")).show()



# COMMAND ----------

df_2018.count()


# COMMAND ----------

df_2018.where(col("CRS_DEP_TIME").isNotNull()).count()


# COMMAND ----------

df_2018.where(col("FL_DATE").isNotNull()).count()


# COMMAND ----------

df_2018.where(col("origin").isNotNull()).count()


# COMMAND ----------

df_2018.where(col("dest").isNotNull()).count()


# COMMAND ----------

from pyspark.sql.functions import quarter,concat,lit
df_2018=df_2018.withColumn("quarter",concat(lit("Q"), quarter("FL_DATE"))) 


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat_ws, col, lit

# Convert back to full datetime for comparison
df_2018 = df_2018 \
    .withColumn("CRS_DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("CRS_ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_ARR_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("ARR_TIME")), "yyyy-MM-dd HH:mm"))


# COMMAND ----------

from pyspark.sql.functions import (col, expr)

df_2018 = df_2018 \
    .withColumn("DEP_DELAY_COMPUTED", (col("DEP_DATETIME").cast("long") - col("CRS_DEP_DATETIME").cast("long")) / 60) \
    .withColumn("ARR_DELAY_COMPUTED", (col("ARR_DATETIME").cast("long") - col("CRS_ARR_DATETIME").cast("long")) / 60)


# COMMAND ----------

#finding inconsistent records store these records in a separate data frame and upload it in aws s3 path.
df_2018_inconsistent_record=df_2018.select(
    "FL_DATE", "ORIGIN", "DEST",
    "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "DEP_DELAY_COMPUTED",
    "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "ARR_DELAY_COMPUTED"
).where((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0)) 



# COMMAND ----------

df_2018_inconsistent_record.write.mode("overwrite").parquet("s3a://endtoenddataengineerproject/inconsistent_records/2018/")
df_2018_inconsistent_record.write.mode("overwrite").parquet("dbfs:/project/incosistent_records/2018/")


# COMMAND ----------

#finding inconsistent records store these records in a separate data frame and upload it in aws s3 path.
df_2018_inconsistent_records= df_2018.filter(
    ~((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))
)

# COMMAND ----------

#remove these records from the dataset i.e., delete it 
from pyspark.sql.functions import col

df_inconsistent = df_2018_inconsistent_records.filter(~((col("CANCELLED") == 1) & (col("DEP_TIME").isNotNull())))

# COMMAND ----------

from pyspark.sql.functions import month
df_inconsistent=df_inconsistent.withColumn("month",month("FL_DATE"))

# COMMAND ----------

df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("s3a://endtoenddataengineerproject/processed/2018/")
df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("dbfs:/project/processed/2018/")


# COMMAND ----------

df_2020=spark.read.csv('dbfs:/project/raw/2020.csv',header=True,inferSchema=True)
df_2020.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col
df_2020=df_2020.withColumn("CRS_DEP_TIME",col("CRS_DEP_TIME").cast("string"))\
    .withColumn("DEP_TIME",col("DEP_TIME").cast("string"))\
        .withColumn("CRS_ARR_TIME",col("CRS_ARR_TIME").cast("string"))\
            .withColumn("ARR_TIME",col("ARR_TIME").cast("string"))
df_2020.printSchema()



# COMMAND ----------

from pyspark.sql.functions import col,lpad,concat_ws
df_2020=df_2020.withColumn("CRS_DEP_TIME",lpad(col("CRS_DEP_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_DEP_TIME",concat_ws(":",col("CRS_DEP_TIME").substr(1,2),col("CRS_DEP_TIME").substr(3,2)))\
    .withColumn("DEP_TIME",lpad(col("DEP_TIME").cast("string"),4,"0"))\
    .withColumn("DEP_TIME",concat_ws(":",col("DEP_TIME").substr(1,2),col("DEP_TIME").substr(3,2)))\
    .withColumn("CRS_ARR_TIME",lpad(col("CRS_ARR_TIME").cast("string"),4,"0"))\
    .withColumn("CRS_ARR_TIME",concat_ws(":",col("CRS_ARR_TIME").substr(1,2),col("CRS_ARR_TIME").substr(3,2)))\
    .withColumn("ARR_TIME",lpad(col("ARR_TIME").cast("string"),4,"0"))\
        .withColumn("ARR_TIME",concat_ws(":",col("ARR_TIME").substr(1,2),col("ARR_TIME").substr(3,2)))


# COMMAND ----------

from pyspark.sql.functions import upper
df_2020.select(upper("origin").alias("origin"),
               upper("dest").alias("dest")).show()


# COMMAND ----------

df_2020.count()


# COMMAND ----------

df_2020.where(col("CRS_DEP_TIME").isNotNull()).count()


# COMMAND ----------

df_2020.where(col("FL_DATE").isNotNull()).count()


# COMMAND ----------

df_2020.where(col("origin").isNotNull()).count()


# COMMAND ----------

df_2020.where(col("dest").isNotNull()).count()


# COMMAND ----------

from pyspark.sql.functions import quarter,concat,lit
df_2020=df_2020.withColumn("quarter",concat(lit("Q"), quarter("FL_DATE"))) 


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat_ws, col, lit

# Convert back to full datetime for comparison
df_2020 = df_2020 \
    .withColumn("CRS_DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("DEP_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("DEP_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("CRS_ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("CRS_ARR_TIME")), "yyyy-MM-dd HH:mm")) \
    .withColumn("ARR_DATETIME", to_timestamp(concat_ws(" ", col("FL_DATE"), col("ARR_TIME")), "yyyy-MM-dd HH:mm"))


# COMMAND ----------

from pyspark.sql.functions import (col, expr)

df_2020 = df_2020 \
    .withColumn("DEP_DELAY_COMPUTED", (col("DEP_DATETIME").cast("long") - col("CRS_DEP_DATETIME").cast("long")) / 60) \
    .withColumn("ARR_DELAY_COMPUTED", (col("ARR_DATETIME").cast("long") - col("CRS_ARR_DATETIME").cast("long")) / 60)


# COMMAND ----------

#finding inconsistent records store these records in a separate data frame and upload it in aws s3 path.
df_2020_inconsistent_record=df_2020.select(
    "FL_DATE", "ORIGIN", "DEST",
    "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "DEP_DELAY_COMPUTED",
    "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "ARR_DELAY_COMPUTED"
).where((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))


# COMMAND ----------

df_2020_inconsistent_record.write.mode("overwrite").parquet("s3a://endtoenddataengineerproject/inconsistent_records/2020/")
df_2020_inconsistent_record.write.mode("overwrite").parquet("dbfs:/project/incosistent_records/2020/")


# COMMAND ----------

#finding inconsistent records store these records in a separate data frame and upload it in aws s3 path.
df_2020_inconsistent_records= df_2020.filter(
    ~((col("DEP_DELAY") < 0) & (col("DEP_DELAY_COMPUTED") > 0))
)



# COMMAND ----------

#remove these records from the dataset i.e., delete it 
from pyspark.sql.functions import col

df_inconsistent = df_2020_inconsistent_records.filter(~((col("CANCELLED") == 1) & (col("DEP_TIME").isNotNull())))


# COMMAND ----------

from pyspark.sql.functions import month
df_inconsistent=df_inconsistent.withColumn("month",month("FL_DATE"))

# COMMAND ----------

df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("s3a://endtoenddataengineerproject/processed/2020/")
df_inconsistent.write.partitionBy("month","OP_CARRIER")\
    .mode("overwrite")\
        .parquet("dbfs:/project/processed/2020/")
