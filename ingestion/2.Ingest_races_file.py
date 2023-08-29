# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest data in races file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. read csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read \
    .option('header', True) \
        .schema(races_schema) \
            .csv('abfss://raw@formula1ls84.dfs.core.windows.net/races.csv')

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2. Add ingestion date race_timestamp to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

race_df_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))


# COMMAND ----------

display(race_df_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Select only the columns required

# COMMAND ----------

races_selected_column = race_df_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                         col('round'), col('circuitId').alias('circuit_id'), col('name').alias('race_name'),
                                                         col('ingestion_date'), col('race_timestamp'))
                            

# COMMAND ----------

display(races_selected_column)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write the output processed container in parquet format

# COMMAND ----------

races_selected_column.write.mode('overwrite').partitionBy('race_year').parquet('abfss://processed@formula1ls84.dfs.core.windows.net/races')

# COMMAND ----------

display(spark.read.parquet('abfss://processed@formula1ls84.dfs.core.windows.net/races'))