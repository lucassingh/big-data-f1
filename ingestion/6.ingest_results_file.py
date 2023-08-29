# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest result json file

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

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json('abfss://raw@formula1ls84.dfs.core.windows.net/results.json')

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Renamed and new columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3. Drop unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

result_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

display(result_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write output to parquet file
# MAGIC

# COMMAND ----------

display(result_final_df.write.mode('overwrite').parquet("abfss://processed@formula1ls84.dfs.core.windows.net/results"))

# COMMAND ----------

result_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_raw.results")