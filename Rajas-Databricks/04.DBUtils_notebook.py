# Databricks notebook source
# dbutils.fs.help()
dbutils.fs.help('cp')

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/babynames')

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/babynames_test')

# COMMAND ----------

dbutils.fs.cp('dbfs:/FileStore/babynames/baby_names_1960to1965.csv','dbfs:/FileStore/babynames_test')

# COMMAND ----------

dbutils.fs.put('dbfs:/FileStore/babynames_test/test.txt', 'Hi My name is Vinay Kumar M')

display(dbutils.fs.ls("/FileStore/babynames_test/test.txt"))

dbutils.fs.head('dbfs:/FileStore/babynames_test/test.txt')

df = spark.read.text('dbfs:/FileStore/babynames_test/test.txt')
display(df)
