-- Databricks notebook source
drop database if exists hive_metastore.f1_presentation;

create database if not exists hive_metastore.f1_presentation
location '/mnt/acdeadls/presentation';

-- COMMAND ----------

use hive_metastore.f1_presentation;
select current_database()
