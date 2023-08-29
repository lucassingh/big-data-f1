# Databricks notebook source


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <=5")
# tambien se puede
# races_filtered_df = races_df.filter(races_df["race_year"] == 2019)
display(races_filtered_df)