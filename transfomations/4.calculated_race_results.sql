-- Databricks notebook source
-- MAGIC %run "/formula-1-project/includes/configuration"

-- COMMAND ----------

show databases

-- COMMAND ----------

use f1_raw

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

select races.year,
        constructors.name,
        drivers.name,
        results.position,
        results.points
  from results
  join drivers ON (results.driver_id = drivers.driverId)
  join constructors ON (results.constructor_id = constructors.constructorId)
  join races ON (results.race_id = races.raceId)