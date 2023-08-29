# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope='formula-1-scope', key='access-adls-using-acces-key')

spark.conf.set(
    "fs.azure.account.key.formula1ls84.dfs.core.windows.net",
    formuladl_account_key
)

display(dbutils.fs.ls("abfss://demo@formula1ls84.dfs.core.windows.net"))



# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %run ../includes/configuration
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Read csv

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope='formula-1-scope', key='access-adls-using-acces-key')

spark.conf.set(
    "fs.azure.account.key.formula1ls84.dfs.core.windows.net",
    formuladl_account_key
)

display(dbutils.fs.ls("abfss://raw@formula1ls84.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv('abfss://raw@formula1ls84.dfs.core.windows.net/circuits.csv')

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. select only columns we need

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt" )

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    circuits_df.circuitId, 
    circuits_df.circuitRef, 
    circuits_df.name, 
    circuits_df.location, 
    circuits_df.country, 
    circuits_df.lat, 
    circuits_df.lng, 
    circuits_df.alt
)

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    circuits_df["circuitId"], 
    circuits_df["circuitRef"], 
    circuits_df["name"], 
    circuits_df["location"], 
    circuits_df["country"], 
    circuits_df["lat"], 
    circuits_df["lng"], 
    circuits_df["alt"]
)

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

## for change name column you can use alisas for example col("country").alias("race_country"),
circuits_selected_df = circuits_df.select(
    col("circuitId"),
    col("circuitRef"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt"),
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 4. Rename columns

# COMMAND ----------

circuits_rename_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitId", "circuit_id") \
        .withColumnRenamed("circuitRef", "circuit_ref") \
            .withColumnRenamed("lat", "latitude") \
                .withColumnRenamed("lng", "longitude") \
                    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

display(circuits_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add ingestion date to dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_rename_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read proceess storage 

# COMMAND ----------

# MAGIC %fs
# MAGIC ls abfss://processed@formula1ls84.dfs.core.windows.net/circuits

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write data to datalake as a parket

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")