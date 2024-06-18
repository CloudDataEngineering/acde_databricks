# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv

# COMMAND ----------

adls_account_key = dbutils.secrets.get(scope= 'lti-scope', key = 'acdeadls')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.acdeadls.dfs.core.windows.net",
    adls_account_key)

# COMMAND ----------

dbutils.fs.ls('abfss://demo@acdeadls.dfs.core.windows.net')
# display(dbutils.fs.ls('abfss://demo@acdeadls.dfs.core.windows.net'))

# COMMAND ----------

import pandas as pd
df_circuits = pd.read_csv('https://raw.githubusercontent.com/CloudDataEngineering/acde_databricks/main/raw/files/formula1/circuits.csv')
# display(df_circuits)

# COMMAND ----------

df = spark.createDataFrame(df_circuits)
df.write.format('csv').option('header', 'true').mode('overwrite').save('abfss://demo@acdeadls.dfs.core.windows.net/circuits.csv')

# COMMAND ----------

display(spark.read.option('header', 'true').csv('abfss://demo@acdeadls.dfs.core.windows.net/circuits.csv').limit(10))
