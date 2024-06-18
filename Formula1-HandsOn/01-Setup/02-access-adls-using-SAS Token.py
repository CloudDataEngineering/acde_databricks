# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv

# COMMAND ----------

# dbutils.secrets.listScopes()
# display(dbutils.secrets.list(scope="lti-scope"))
adls_sas_key = dbutils.secrets.get(scope='lti-scope', key= 'adls-sas-key')

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
# spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))

spark.conf.set("fs.azure.account.auth.type.acdeadls.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.acdeadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.acdeadls.dfs.core.windows.net", adls_sas_key)

# COMMAND ----------

dbutils.fs.ls('abfss://demo@acdeadls.dfs.core.windows.net')

# COMMAND ----------

display(spark.read.option("header", "true").csv('abfss://demo@acdeadls.dfs.core.windows.net/circuits.csv'))
