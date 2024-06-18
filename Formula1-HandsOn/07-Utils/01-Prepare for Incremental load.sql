-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Drop all the tables

-- COMMAND ----------

-- select current_database()
-- use hive_metastore.f1_processed;
-- show databases;
show tables;

-- COMMAND ----------

drop database if exists hive_metastore.f1_processed cascade;

create database if not exists hive_metastore.f1_processed
location '/mnt/acdeadls/processed';

show databases;

-- COMMAND ----------

use hive_metastore.f1_processed;
show tables;

-- COMMAND ----------

drop database if exists f1_presentation cascade;

create database if not exists f1_presentation
location '/mnt/acdeadls/presentation';

use hive_metastore.f1_presentation;
show tables;
