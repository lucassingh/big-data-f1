# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #1. Inner Join

# COMMAND ----------

race_circuits = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")

# COMMAND ----------

display(race_circuits)

# COMMAND ----------

# MAGIC %md 
# MAGIC #2. Outer Join

# COMMAND ----------

# MAGIC %md
# MAGIC ### left outer join

# COMMAND ----------

race_circuits = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")