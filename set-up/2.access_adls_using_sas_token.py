# Databricks notebook source
# MAGIC %md 
# MAGIC #### Access Azure Data Lake using keys
# MAGIC 1. Set spark config for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data deom circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1ls84.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1ls84.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1ls84.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="sp=rl&st=2023-08-22T19:45:16Z&se=2023-08-23T03:45:16Z&spr=https&sv=2022-11-02&sr=c&sig=cV5jUutD7udLVTPbgAV%2B2ktPSp32RxXpKtWehGsk83Q%3D"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1ls84.dfs.core.windows.net"))

# COMMAND ----------

df = display(spark.read.csv("abfss://demo@formula1ls84.dfs.core.windows.net/circuits.csv"))
df

# COMMAND ----------

