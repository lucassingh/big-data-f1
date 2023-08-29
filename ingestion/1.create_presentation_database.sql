-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "dbfs:/user/hive/warehouse/f1_processed.db"

-- COMMAND ----------

desc database f1_processed