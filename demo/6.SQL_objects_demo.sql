-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. spark sql documentation
-- MAGIC 2. create database demo
-- MAGIC 3. data tab in the UI
-- MAGIC 4. show command
-- MAGIC 5. describe  comand
-- MAGIC 6. find the current  database

-- COMMAND ----------

CREATE DATABASE  demo;

-- COMMAND ----------

create database if not exists demo

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database demo

-- COMMAND ----------

use demo;

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_py")

-- COMMAND ----------

show tables

-- COMMAND ----------

describe race_results_py

-- COMMAND ----------

select * from demo.race_results_py where race_year = 2019

-- COMMAND ----------

create table race_results_sql 
as
select * from demo.race_results_py
where race_year = 2020

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. creating external table using python
-- MAGIC 2. creating external table using sql
-- MAGIC 3. effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql(
race_year int,
race_name string,
race_date timestamp,
circuit_location string,
driver_name string,
driver_number int,
driver_nationality string,
team string,
grid int,
fastest_lap int,
race_time string,
points float,
position int,
created_date timestamp
)
using parquet
location "abfss://presentation@formula1ls84.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * from demo.race_results_ext_py where race_year = 2020

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. create temp view
-- MAGIC 2. create global tempp view
-- MAGIC 3. create permanent view

-- COMMAND ----------

create or replace global temp view v_race_results
as
select * from demo.race_results_py
where race_year = 2018

-- COMMAND ----------

select * from global_temp.v_race_results

-- COMMAND ----------

show tables in global_temp