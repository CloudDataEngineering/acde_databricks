# Databricks notebook source
# MAGIC %run "../03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Produce driver standings

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standing_df = race_results_df.groupBy('race_year', 'driver_name', 'driver_nationality', 'team_name') \
    .agg(sum('points').alias('Total_points'),count(when(col('position') == 1, True)).alias('Wins'))

display(driver_standing_df.filter('race_year == 2018'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spek = Window.partitionBy('race_year').orderBy(desc('Total_points'), desc('Wins'))
final_df = driver_standing_df.withColumn('Rank', rank().over(driver_rank_spek))
display(final_df.select('Rank', 'race_year', 'driver_name', 'driver_nationality', 'Total_points', 'Wins', 'team_name'))

# COMMAND ----------

# final_df.write.parquet(f"{presentation_folder_path}/driver_standing")

final_df.write.mode('overwrite').format('parquet').saveAsTable("hive_metastore.f1_presentation.driver_standing")

display(spark.read.parquet(f"{presentation_folder_path}/driver_standing").limit(10))
