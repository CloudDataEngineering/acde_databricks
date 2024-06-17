-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

-- MAGIC %run "/Workspace/Users/azureclouddataengineering@gmail.com/Formula1-HandsOn/03-Includes/01-Configuration"

-- COMMAND ----------

-- drop database if exists hive_metastore.demo cascade
-- CREATE DATABASE hive_metastore.demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hive_metastore.demo;

-- COMMAND ----------

use hive_metastore.demo;

-- COMMAND ----------

-- show databases
-- describe database demo
describe database extended demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 1. Create managed table using SQL
-- MAGIC 1. Effect of dropping a managed table
-- MAGIC 1. Describe table 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # display(dbutils.fs.mounts())
-- MAGIC display(dbutils.fs.ls('/mnt/adlsacde/presentation'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')
-- MAGIC display(race_results_df.limit(10))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').saveAsTable('demo.race_results_df_python')

-- COMMAND ----------

-- show tables
-- show tables in demo
-- describe  table race_results_df_python
-- DESC race_results_df_python
desc extended race_results_df_python
-- select * from race_results_df_python limit 10

-- COMMAND ----------

select * from demo.race_results_df_python where race_year = 2020 limit 10;

-- COMMAND ----------

create table demo.race_results_df_sql
as
select * from demo.race_results_df_python;

-- COMMAND ----------



-- COMMAND ----------

-- select current_database()
-- desc schema extended demo
-- show tables;
desc extended race_results_df_sql

-- COMMAND ----------

drop table demo.race_results_df_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External Tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 1. Create external table using SQL
-- MAGIC 1. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # display(race_results_df)
-- MAGIC race_results_df.write.format('parquet').option('path',f"{presentation_folder_path}/race_results_ext_py").saveAsTable('race_results_ext_py')

-- COMMAND ----------

-- select * from race_results_ext_py
-- show tables
desc extended demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

create table demo.race_results_ext_sql
(
  race_year	int,
race_name	string,
race_date	timestamp,
circuit_location	string,
driver_name	string,
driver_number	int,
driver_nationality	string,
team_name	string,
grid	int,
fastest_lap	int,
race_time	string,
points	float,
position	int,
created_date	timestamp
)
using parquet
location '/mnt/adlsacde/presentation/race_results_ext_sql'

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

insert into race_results_ext_sql
select * from demo.race_results_ext_py

-- COMMAND ----------

select * from demo.race_results_ext_sql limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 1. Create Global Temp View
-- MAGIC 1. Create Permanent View

-- COMMAND ----------

select current_database()

-- COMMAND ----------

create or replace temp view v_race_results
as 
select * from demo.race_results_df_python
where
race_year = 2020;

select * from v_race_results;

-- COMMAND ----------

create or replace global temp view gv_race_results
as 
select * from demo.race_results_df_python
where
race_year = 2018;

-- COMMAND ----------

select * from global_temp.gv_race_results limit 10;

-- COMMAND ----------

-- show tables
show tables in global_temp;

-- COMMAND ----------

create or replace view pv_race_results
as 
select * from demo.race_results_df_python
where
race_year = 2000;

select * from pv_race_results limit 10;

-- COMMAND ----------

show tables in demo;
-- drop view v_race_results;
