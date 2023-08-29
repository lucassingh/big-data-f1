# Databricks notebook source
# MAGIC %md
# MAGIC ##Explore the capabilities of the dbuitls.secrets utility

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope ='formula-1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula-1-scope', key='access-adls-using-acces-key')