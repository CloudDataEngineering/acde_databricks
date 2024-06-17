# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/azureclouddataengineering@gmail.com/Formula1-HandsOn/03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/azureclouddataengineering@gmail.com/Formula1-HandsOn/03-Includes/02-common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

race_year_list = [row['race_year'] for row in race_results_df.select('race_year').collect()]

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# overwrite_partition(final_df, 'hive_metastore', 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name and tgt.race_year = src.race_year"
# merge_delta_data(input_df, schema_name, db_name, table_name, folder_path, merge_condition, partition_column)
merge_delta_data(final_df, 'hive_metastore', 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition,  'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select current_database()
# MAGIC use hive_metastore.f1_presentation;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from hive_metastore.f1_presentation.driver_standings;
# MAGIC select 
# MAGIC race_year, count(*)
# MAGIC from 
# MAGIC hive_metastore.f1_presentation.driver_standings 
# MAGIC group by all
# MAGIC order by race_year desc;

# COMMAND ----------

dbutils.notebook.exit('Success')