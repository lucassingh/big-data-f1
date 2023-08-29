# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest laps csv's file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f'abfss://raw@formula1ls84.dfs.core.windows.net/lap_times')

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Renamed and new columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write output to parquet file
# MAGIC

# COMMAND ----------

display(final_df.write.mode('overwrite').parquet("abfss://processed@formula1ls84.dfs.core.windows.net/lap_times"))