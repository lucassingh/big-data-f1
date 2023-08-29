-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. spark sql documentation
-- MAGIC 2. create database demo
-- MAGIC 3. data tab in the UI
-- MAGIC 4. show command
-- MAGIC 5. describe  comand
-- MAGIC 6. find the current  database

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

show databases

-- COMMAND ----------

use f1_raw

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

desc drivers