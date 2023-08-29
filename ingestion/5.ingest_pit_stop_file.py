# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest pit stop json file

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope='formula-1-scope', key='access-adls-using-acces-key')

spark.conf.set(
    "fs.azure.account.key.formula1ls84.dfs.core.windows.net",
    formuladl_account_key
)

display(dbutils.fs.ls("abfss://raw@formula1ls84.dfs.core.windows.net"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. read json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])


# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
    .option("multiline", True) \
.json('abfss://raw@formula1ls84.dfs.core.windows.net/pit_stops.json')

# COMMAND ----------

pit_stops_df.printSchema()

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Renamed and new columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "racer_id") \
        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write output to parquet file
# MAGIC

# COMMAND ----------

final_df.write.mode('overwrite').parquet("abfss://processed@formula1ls84.dfs.core.windows.net/pit_stops")
display(final_df)