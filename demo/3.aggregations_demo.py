# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Aggregate functions

# COMMAND ----------

demod_df = race_results_df.filter("race_year=2020")

# COMMAND ----------

display(demod_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demod_df.select(count("*")).show()

# COMMAND ----------

demod_df.select(count("race_name")).show()

# COMMAND ----------

demod_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demod_df.select(sum("points")).show()

# COMMAND ----------

demod_df.filter("driver_name ='Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demod_df.filter("driver_name ='Lewis Hamilton'").select(sum("points"), countDistinct('race_name')) \
    .withColumnRenamed("sum(points)", "total_points") \
        .withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
    .show()

# COMMAND ----------

demod_df\
    .groupBy("driver_name") \
    .agg(sum("points"), countDistinct("race_name").alias("number_of_races")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### windows Functions

# COMMAND ----------

demod_df = race_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped = demod_df\
    .groupBy("race_year", "driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_grouped)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped.withColumn("rank", rank().over(driverRankSpec)).show(100)