# Databricks notebook source
# MAGIC %md 
# MAGIC #### Access Azure Data Lake using keys
# MAGIC 1. Set spark config for service principal
# MAGIC 2. List files from demo container
# MAGIC 3. Read data deom circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1ls84.dfs.core.windows.net"))

# COMMAND ----------

df = display(spark.read.csv("abfss://demo@formula1ls84.dfs.core.windows.net/circuits.csv"))
df

# COMMAND ----------

