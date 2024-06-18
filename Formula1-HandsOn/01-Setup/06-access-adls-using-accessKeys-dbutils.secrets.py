# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv

# COMMAND ----------

# dbutils.secrets.listScopes()
# dbutils.secrets.list("lti-scope")
# dbutils.secrets.get(scope="lti-scope", key="acdeadls")
# formula1dl_accountkey = dbutils.secrets.get(scope="lti-scope", key="acdeadls")
# display(formula1dl_accountkey)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.acdeadls.dfs.core.windows.net",
    formula1dl_accountkey)

# COMMAND ----------

# dbutils.fs.ls('abfss://demo@acdeadls.dfs.core.windows.net')
display(dbutils.fs.ls('abfss://demo@acdeadls.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.option('header', 'true').csv('abfss://demo@acdeadls.dfs.core.windows.net/circuits.csv'))
