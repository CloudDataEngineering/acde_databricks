-- Databricks notebook source
drop database if exists hive_metastore.f1_processed;

create database if not exists hive_metastore.f1_processed
location '/mnt/acdeadls/processed' ;

-- COMMAND ----------

-- use hive_metastore.f1_processed;
-- show databases;
describe database f1_processed;
