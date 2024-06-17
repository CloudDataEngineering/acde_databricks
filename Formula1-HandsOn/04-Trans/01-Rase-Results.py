# Databricks notebook source
# MAGIC %run "/Workspace/Users/azureclouddataengineering@gmail.com/Formula1-HandsOn/03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read all the data as required

# COMMAND ----------

driver_df = spark.read.parquet(f'{processed_folder_path}/driver')\
                    .withColumnRenamed('number', 'driver_number')\
                    .withColumnRenamed('name', 'driver_name')\
                    .withColumnRenamed('nationality', 'driver_nationality')
# display(driver_df.limit(10))

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')\
                        .withColumnRenamed('location', 'circuit_location')
# display(circuits_df.limit(10))

# COMMAND ----------

constructor_df = spark.read.parquet(f'{processed_folder_path}/constructor')\
                        .withColumnRenamed('name', 'team_name')
# display(constructor_df.limit(10))

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')\
                    .withColumnRenamed('name', 'race_name')\
                    .withColumnRenamed('race_timestamp', 'race_date')
# display(races_df.limit(10))

# COMMAND ----------

results_df = spark.read.parquet(f'{processed_folder_path}/results')\
                    .withColumnRenamed('time', 'race_time')
# display(results_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join Circuits to race

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'inner')\
  .select(races_df.race_id, races_df.race_name, races_df.race_date, races_df.race_year, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id, 'inner') \
                            .join(driver_df, results_df.driver_id == driver_df.driver_id, 'inner') \
                            .join(constructor_df, results_df.constructor_id == constructor_df.constructor_id, 'inner')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = race_results_df.select('race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality', 'team_name', 'grid', 'fastest_lap', 'race_time', 'points', 'position') \
    .withColumn('created_date', current_timestamp())

display(final_df.limit(10))

# COMMAND ----------

display(final_df.filter('race_year == 2020 and race_name == "Abu Dhabi Grand Prix"').orderBy(final_df.points.desc()).limit(10))

# COMMAND ----------

# final_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/race_results')
final_df.write.mode('overwrite').format('parquet').saveAsTable('hive_metastore.f1_presentation.race_results')

display(spark.read.parquet(f'{presentation_folder_path}/race_results'))
