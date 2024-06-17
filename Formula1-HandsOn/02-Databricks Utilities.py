# Databricks notebook source
# MAGIC %fs
# MAGIC ls /databricks-datasets/

# COMMAND ----------

dbutils.fs.ls("dbfs:/databricks-datasets/COVID/covid-19-data/")

# COMMAND ----------

for files in dbutils.fs.ls("dbfs:/databricks-datasets/COVID/covid-19-data/"):
    print(files)


# COMMAND ----------

for files in dbutils.fs.ls("dbfs:/databricks-datasets/COVID/"):
    if files.name.endswith("/"):
        print(files.name)


# COMMAND ----------

# dbutils.help()
dbutils.fs.help()
