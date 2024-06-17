# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.adlsacde.dfs.core.windows.net",
    'Account_key'
)

# COMMAND ----------

# dbutils.fs.ls('abfss://demo@adlsacde.dfs.core.windows.net')
display(dbutils.fs.ls('abfss://demo@adlsacde.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@adlsacde.dfs.core.windows.net/circuits.csv'))
