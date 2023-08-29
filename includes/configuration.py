# Databricks notebook source
formuladl_account_key = dbutils.secrets.get(scope='formula-1-scope', key='access-adls-using-acces-key')

spark.conf.set(
    "fs.azure.account.key.formula1ls84.dfs.core.windows.net",
    formuladl_account_key
)

dbutils.fs.ls("abfss://demo@formula1ls84.dfs.core.windows.net")

# COMMAND ----------

raw_folder_path = 'abfss://raw@formula1ls84.dfs.core.windows.net'
processed_folder_path = 'abfss://processed@formula1ls84.dfs.core.windows.net'
presentation_folder_path = 'abfss://presentation@formula1ls84.dfs.core.windows.net'

# COMMAND ----------

