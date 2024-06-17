# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM v_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results where race_year = 2009 limit 10;
