# Databricks notebook source
# MAGIC %md
# MAGIC ##DBUtils fs help

# COMMAND ----------

# dbutils.fs.help()
dbutils.fs.help('cp')

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils Notebook Help

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils Widgets Help

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils Secrets Help

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils File System Commands

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/babynames')

# COMMAND ----------

dbutils.fs.head('dbfs:/FileStore/babynames/baby_names_1960to1965.csv')

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/tables/babynames')

# COMMAND ----------

dbutils.fs.cp('dbfs:/FileStore/babynames/baby_names_1960to1965.csv','dbfs:/FileStore/babynames_test')

# COMMAND ----------

dbutils.fs.mv("/FileStore/babynames/baby_names_1971to1975.csv","/FileStore/babynames_test")

# COMMAND ----------

# dbutils.fs.put('dbfs:/FileStore/babynames_test/test.txt', 'Hi My name is Vinay Kumar M')

# display(dbutils.fs.ls("/FileStore/babynames_test/test.txt"))

dbutils.fs.head('dbfs:/FileStore/babynames_test/test.txt')

# df = spark.read.text('dbfs:/FileStore/babynames_test/test.txt')
# display(df)

# COMMAND ----------

# dbutils.fs.rm('dbfs:/FileStore/babynames_test/test.txt')
dbutils.fs.rm('dbfs:/FileStore/tables/Production_data_corrupt.csv' ,True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils Notebook Commands

# COMMAND ----------

dbutils.notebook.run('04.DBUtils_notebook', 60)

# COMMAND ----------

# MAGIC %md
# MAGIC #Widgets

# COMMAND ----------

# MAGIC %run ./05.Training_Widget $Folder_Name = "dbfs:/FileStore/babynames/" $File_Name = "baby_names_1960to1965.csv"

# COMMAND ----------

# dbutils.widgets.dropdown('Drop_down', '1', [str(x) for x in range(1, 10)])

# dbutils.widgets.combobox('Combo_box', '1', [str(x) for x in range(1, 10)])

# dbutils.widgets.multiselect('Product', 'Camera', ('Camera', 'GPS', 'SmartPhone'))

# COMMAND ----------

# dbutils.widgets.remove('Combo_box')

# dbutils.widgets.remove('Drop_down')

# dbutils.widgets.remove('Product')

# COMMAND ----------

dbutils.widgets.removeAll()
