# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest driver json file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True),
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)                
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json('abfss://raw@formula1ls84.dfs.core.windows.net/drivers.json')

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

drivers_dropped_df = drivers_df.drop(drivers_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3. Rename columns and add ingestion date 

# COMMAND ----------

driver_with_column_final_df = drivers_dropped_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
        .withColumn("ingestion_date", current_timestamp()) \
            .withColumn("driver_name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(driver_with_column_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write output to parquet file
# MAGIC

# COMMAND ----------

display(driver_with_column_final_df.write.mode('overwrite').parquet("abfss://processed@formula1ls84.dfs.core.windows.net/drivers"))