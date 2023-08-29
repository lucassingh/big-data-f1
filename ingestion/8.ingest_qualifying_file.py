# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest qualifying json file

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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
    .option("multiline", True) \
.json('abfss://raw@formula1ls84.dfs.core.windows.net/qualifying')

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Renamed and new columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Write output to parquet file
# MAGIC

# COMMAND ----------

display(final_df.write.mode('overwrite').parquet("abfss://processed@formula1ls84.dfs.core.windows.net/qualifying"))