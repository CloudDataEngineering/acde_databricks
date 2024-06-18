-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Races Table

-- COMMAND ----------

-- Drop the database if it exists
DROP DATABASE IF EXISTS hive_metastore.f1_raw;

-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS hive_metastore.f1_raw;

-- Use the database
USE hive_metastore.f1_raw;

-- Select the current database
SELECT current_database();

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # display(dbutils.fs.mounts())
-- MAGIC # display(dbutils.fs.ls('/mnt/acdeadls/raw'))
-- MAGIC # display(spark.read.option('header', 'true').csv('dbfs:/mnt/acdeadls/raw/circuits.csv').limit(10))
-- MAGIC # circuits = spark.read.option('header', 'true').csv('dbfs:/mnt/acdeadls/raw/circuits.csv').limit(10)
-- MAGIC # circuits.write.format('csv').saveAsTable('hive_metastore.demo.circuits')
-- MAGIC
-- MAGIC # display(spark.sql('select * from hive_metastore.demo.circuits'));
-- MAGIC # display(spark.sql('desc hive_metastore.demo.circuits'))

-- COMMAND ----------

SELECT * FROM csv.`/mnt/acdeadls/raw/circuits.csv` LIMIT 10;

-- COMMAND ----------

drop table if exists f1_raw.circuits;

create table if not exists f1_raw.circuits(
circuitId	int,
circuitRef	string,
name	string,
location string,
country	string,
lat	double,
lng	double,
alt	int,
url	string
)
using csv
options(path '/mnt/acdeadls/raw/circuits.csv', header true);

select * from f1_raw.circuits limit 10;

-- COMMAND ----------

select * from csv.`/mnt/acdeadls/raw/races.csv` limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table

-- COMMAND ----------

drop table if exists f1_raw.races;

create table if not exists f1_raw.races
(
  raceId	int,
  year	int,
  round	int,
  circuitId	int,
  name	string,
  date	date,
  time	string,
  url string
)
using csv
options(path '/mnt/acdeadls/raw/races.csv', header true);

select * from f1_raw.races limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructors table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

-- select * from json.`/mnt/acdeadls/raw/constructors.json` limit 10;

drop table if exists f1_raw.constructors;

create table if not exists f1_raw.constructors
(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
using json
options(path '/mnt/acdeadls/raw/constructors.json');

select * from f1_raw.constructors limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure

-- COMMAND ----------

-- select * from json.`/mnt/acdeadls/raw/drivers.json` limit 10;

drop table if exists f1_raw.drivers;

create table if not exists f1_raw.drivers(
code string,
dob string,
driverId int,
driverRef string,
name struct <forename:string,surname:string>,
nationality string,
number string,
url string
)
using json
options(path '/mnt/acdeadls/raw/drivers.json', header true);

select * from f1_raw.drivers limit 10;


-- COMMAND ----------

-- MAGIC %md ##### Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

-- select * from json.`/mnt/acdeadls/raw/results.json` limit 10;

drop table if exists f1_raw.results;

create table if not exists f1_raw.results
(
constructorId int,
driverId int,
fastestLap string,
fastestLapSpeed string,
fastestLapTime string,
grid int,
laps int,
milliseconds string,
number string,
points double,
position string,
positionOrder int,
positionText int,
raceId int,
rank string,
resultId int,
statusId int,
time string
)
using json
options(path '/mnt/acdeadls/raw/results.json');

select * from f1_raw.results limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

-- select * from json.`/mnt/acdeadls/raw/pit_stops.json` limit 10;

drop table if exists f1_raw.pit_stops;

create table if not exists f1_raw.pit_stops
(
raceId int,
driverId int,
stop int,
lap int,
time string ,
duration string,
milliseconds int
)
using json
options(path '/mnt/acdeadls/raw/pit_stops.json', multiline true);

select * from f1_raw.pit_stops limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files

-- COMMAND ----------

-- SELECT * FROM csv.`/mnt/acdeadls/raw/lap_times/` limit 10;

drop table if exists f1_raw.lap_times;

create table if not exists f1_raw.lap_times
(
raceId int,
driverId int,
lap int,
position int,
time string,
milliseconds int
)
using csv
options(path '/mnt/acdeadls/raw/lap_times');

select * from f1_raw.lap_times limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files

-- COMMAND ----------

-- SELECT * FROM JSON.`/mnt/acdeadls/raw/qualifying/` limit 10;

drop table if exists f1_raw.qualifying;

create table if not exists f1_raw.qualifying
(
constructorId int,
driverId int,
number int,
position int,
q1 string,
q2 string,
q3 string,
qualifyId int,
raceId int
)
using json
options(path '/mnt/acdeadls/raw/qualifying', multiline true);

select * from f1_raw.qualifying limit 10;

-- COMMAND ----------

desc extended f1_raw.circuits;
