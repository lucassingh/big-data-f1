# Databricks notebook source
v_result = dbutils.notebook.run("/formula-1-project/ingestion/1.Ingest_circuit_file", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/formula-1-project/ingestion/2.Ingest_races_file", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/formula-1-project/ingestion/3.ingest_contructors_file", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/formula-1-project/ingestion/4.ingest_drives_file", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/formula-1-project/ingestion/5.ingest_pit_stop_file", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/formula-1-project/ingestion/6.ingest_results_file", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/formula-1-project/ingestion/7.ingest_lap_file", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/formula-1-project/ingestion/8.ingest_qualifying_file", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result