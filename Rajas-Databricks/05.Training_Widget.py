# Databricks notebook source
# dbutils.widgets.removeAll()

dbutils.widgets.text("Folder_Name", "", "")
dbutils.widgets.text("File_Name", "", "")

# COMMAND ----------

Folder_location = dbutils.widgets.get('Folder_Name')
File_locaion = dbutils.widgets.get('File_Name')

print('Folder Variable is : ' , Folder_location)
print('File Variable is :', File_locaion)

# COMMAND ----------

# MAGIC %md
# MAGIC # Read singel CSV file

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep', ',').load(Folder_location+File_locaion)

display(df)
