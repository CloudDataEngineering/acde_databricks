# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS Root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 1. Interact with DBFS File Browser
# MAGIC 1. Upload files DBFS Root

# COMMAND ----------

# dbutils.fs.ls('/')
display(dbutils.fs.ls('/'))

# COMMAND ----------

# display(dbutils.fs.ls('/FileStore/'))
display(spark.read.csv('/FileStore/circuits.csv', header=True))
