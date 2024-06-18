# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_processed.circuits limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT p_catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT p_schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT p_table DEFAULT "";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * From ${p_catalog}.${p_schema}.${p_table} limit 10;
