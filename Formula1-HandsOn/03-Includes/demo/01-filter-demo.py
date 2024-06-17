# Databricks notebook source
# MAGIC %run "/Workspace/Users/azureclouddataengineering@gmail.com/Formula1-HandsOn/03-Includes/01-Configuration"

# COMMAND ----------

# dbutils.fs.ls(f'{processed_folder_path}/races')
races_df = spark.read.parquet(f'{processed_folder_path}/races')
display(races_df.limit(10))

# COMMAND ----------

races_filter_df = races_df.filter('race_year = 2009 and  round = 5')
display(races_filter_df.count())

# COMMAND ----------

races_filter_df = races_df.filter((races_df.race_year == 2009) & (races_df.round == 5))
display(races_filter_df.count())
