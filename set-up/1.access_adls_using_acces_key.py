# Databricks notebook source
# MAGIC %md 
# MAGIC #### Access Azure Data Lake using keys
# MAGIC 1. Set spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data deom circuits.csv file

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope='formula-1-scope', key='access-adls-using-acces-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1ls84.dfs.core.windows.net",
    formuladl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1ls84.dfs.core.windows.net"))

# COMMAND ----------

df = display(spark.read.csv("abfss://demo@formula1ls84.dfs.core.windows.net/circuits.csv"))
df

# COMMAND ----------

