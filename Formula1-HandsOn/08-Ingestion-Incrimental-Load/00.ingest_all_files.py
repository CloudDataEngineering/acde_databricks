# Databricks notebook source
# MAGIC %md
# MAGIC #### Notebook Workflows

# COMMAND ----------

v_result = dbutils.notebook.run("01-Ingest_Circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("02-Ingest_Race_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("03.ingest_constructors_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("04.ingest_drivers_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("05.ingest_results_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("06.ingest_pit_stops_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("07.ingest_lap_times_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("08.ingest_qualifying_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result
