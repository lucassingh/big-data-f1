-- Databricks notebook source
create database if not exists f1_raw

-- COMMAND ----------

-- MAGIC %run ../includes/configuration

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### create circuits table

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
  circuitId int,
  circuitRef string,
  name string,
  location string,
  country string,
  lat double,
  lng double,
  alt int,
  url string
)
using csv
options (path "abfss://raw@formula1ls84.dfs.core.windows.net/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create race tables

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
raceId int,
year int,
round int,
circuitId int,
name string,
date timestamp,
time string,
url string
)
using csv
options (path "abfss://raw@formula1ls84.dfs.core.windows.net/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructor table
-- MAGIC ##### single line JSON
-- MAGIC ##### simple structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
constructorId int,
constructorRef string,
name string,
nationality string
)
using json
options (path 'abfss://raw@formula1ls84.dfs.core.windows.net/constructors.json', header true)

-- COMMAND ----------

selec

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table
-- MAGIC ##### single line JSON
-- MAGIC ##### complex structure

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId int,
driverRef string,
number int,
code string,
name struct<forename:string, surname:string>,
forename string,
surname string,
dob timestamp,
nationality string
)
using json
options (path 'abfss://raw@formula1ls84.dfs.core.windows.net/drivers.json', header true)

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results table
-- MAGIC ##### single line JSON
-- MAGIC ##### simple structure

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId int,
raceId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points float,
laps int,
time string,
milliseconds int,
fastestLap int,
rank int,
fastestLapTime string,
fastestLapSpeed float,
statusId string,
dataSource string
)
using json
options (path 'abfss://raw@formula1ls84.dfs.core.windows.net/results.json', header true)

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pit stops table
-- MAGIC ##### single line JSON
-- MAGIC ##### simple structure

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
racerId int,
driverId int,
stop string,
lap int,
time string,
duration string,
milliseconds int
)
using json
options (path 'abfss://raw@formula1ls84.dfs.core.windows.net/pit_stops.json', header true, multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### CSV file
-- MAGIC ##### Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "abfss://raw@formula1ls84.dfs.core.windows.net/lap_times", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### json file
-- MAGIC ##### Multiline JSON
-- MAGIC ##### multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "abfss://raw@formula1ls84.dfs.core.windows.net/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying;