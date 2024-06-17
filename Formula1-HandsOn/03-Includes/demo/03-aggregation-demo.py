# Databricks notebook source
# MAGIC %run "/Workspace/Users/azureclouddataengineering@gmail.com/Formula1-HandsOn/03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregate functions demo

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Built-in Aggregate functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df.limit(10))

# COMMAND ----------

demo_df = race_results_df.filter('race_year == 2020')
display(demo_df.count())

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, avg, max, min, sum, when
display(demo_df.select(count('*')))

# COMMAND ----------

display(demo_df.select(count('race_name')))

# COMMAND ----------

display(demo_df.select(countDistinct('race_name')))

# COMMAND ----------

display(demo_df.select(sum('points')))

# COMMAND ----------

demo_df.filter('driver_name = "Lewis Hamilton"').select(sum('points')).show()

# COMMAND ----------

display(demo_df.filter('driver_name = "Lewis Hamilton"').select(sum('points'),countDistinct('race_name'))\
                                                .withColumnRenamed('sum(points)','total_points')\
                                                .withColumnRenamed("count(Distinct race_name)",'total_races'))
                                                # .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### groupBy

# COMMAND ----------

# display(demo_df.groupBy('driver_name').agg(sum('points').alias('total_points'),countDistinct('race_name').alias('total_races')).orderBy('total_points',ascending=False))

display(
demo_df.groupBy('driver_name')\
    .agg(sum('points').alias('total_points')\
    ,countDistinct('race_name').alias('total_races'))\
    .orderBy('total_points',ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Window Functions

# COMMAND ----------

demo_df = race_results_df.filter('race_year in (2018,2019,2020)')
display(demo_df.filter('race_year = 2018'))

# COMMAND ----------

demo_grouped_df = demo_df.groupBy('race_year', 'driver_name') \
                        .agg(sum('points').alias('total_points') \
                        , countDistinct('race_name').alias('total_races')) \
                        .orderBy('total_points', ascending=False)

display(demo_grouped_df.limit(10))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

driverRankSpeck = Window.partitionBy('race_year').orderBy(desc('total_points'))
demo_grouped_df \
    .withColumn('rank', rank().over(driverRankSpeck)) \
    .filter('rank <= 5') \
    .show()
