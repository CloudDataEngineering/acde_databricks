# Databricks notebook source
# MAGIC %md
# MAGIC #### Access dataframes using SQL
# MAGIC ##### Objectives
# MAGIC 1. Create temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "/Workspace/Users/azureclouddataengineering@gmail.com/Formula1-HandsOn/03-Includes/01-Configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results where race_year = 2009 limit 10;

# COMMAND ----------

p_race_year = 2009

# COMMAND ----------

race_year = spark.sql(f"SELECT * FROM v_race_results where race_year = {p_race_year}")
display(race_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Global Temporary Views
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Acesss the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW TABLES;
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results where race_year = 2009 limit 10;

# COMMAND ----------

race_year = spark.sql(f"SELECT * FROM global_temp.gv_race_results where race_year = {p_race_year}")
display(race_year)
