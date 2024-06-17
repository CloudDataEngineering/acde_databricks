-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Query data via unity catalog using 3 level namespace

-- COMMAND ----------

select * from demo_catalog.demo_schema.circuits limit 10;

-- COMMAND ----------

-- show databases;
-- select current_database();

-- show catalogs;
-- select current_catalog();
-- use catalog demo_catalog;

-- show schemas;
-- select current_schema();
-- use schema demo_schema;

-- COMMAND ----------

use catalog demo_catalog;
use schema demo_schema;
select * from circuits limit 10;

-- COMMAND ----------

-- show tables;
DESCRIBE EXTENDED demo_catalog.demo_schema.circuits;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql('show tables'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table('demo_catalog.demo_schema.circuits')
-- MAGIC display(df.limit(10))
