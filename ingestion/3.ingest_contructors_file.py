# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructor json file

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json('abfss://raw@formula1ls84.dfs.core.windows.net/constructors.json')

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3. Rename columns and add ingestion date 

# COMMAND ----------

constructor_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Write output to parquet file
# MAGIC

# COMMAND ----------

display(constructor_final_df.write.mode('overwrite').parquet("abfss://processed@formula1ls84.dfs.core.windows.net/constructors"))