-- Databricks notebook source
use hive_metastore.demo

-- COMMAND ----------

-- show databases;
-- select current_database()
use f1_processed;

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from f1_processed.driver limit 10;

-- COMMAND ----------

desc driver;
-- describe extended driver;

-- COMMAND ----------

select * from f1_processed.driver where nationality = 'British' limit 10;
