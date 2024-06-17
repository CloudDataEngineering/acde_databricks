# Databricks notebook source
# dbutils.notebook.help()
v_result = dbutils.notebook.run('01-Ingest_Circuits_file', 0, {"p_data_source":"Eargast Api"})

# COMMAND ----------

v_result
