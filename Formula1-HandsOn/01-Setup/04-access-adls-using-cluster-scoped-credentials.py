# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster-scoped-credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv

# COMMAND ----------

# dbutils.fs.ls('abfss://demo@adlsacde.dfs.core.windows.net')
display(dbutils.fs.ls('abfss://demo@adlsacde.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@adlsacde.dfs.core.windows.net/circuits.csv'))