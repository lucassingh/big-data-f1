# Databricks notebook source
# MAGIC %md
# MAGIC ##### Access  dataframes using SQL
# MAGIC ###### Objetives
# MAGIC 1. Create temporary views
# MAGIC 2. access the views
# MAGIC 3. access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1)
# MAGIC from v_race_results
# MAGIC where race_year =2020

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)