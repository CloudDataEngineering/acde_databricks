# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source= dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','')
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %run "../03-Includes/02-common_functions"

# COMMAND ----------

# spark.read.json('dbfs:/mnt/adlsacde/raw/2021-03-21/results.json').createOrReplaceTempView('vwResults')

# COMMAND ----------

# %sql
# select raceid,count(1) from vwResults
# group by raceid
# order by raceid desc limit 10;

# COMMAND ----------

# spark.read.json('dbfs:/mnt/adlsacde/raw/2021-03-28/results.json').createOrReplaceTempView('vwResults_w1')

# COMMAND ----------

# %sql
# select raceid,count(1) from vwResults_w1
# group by raceid
# order by raceid desc limit 10;

# COMMAND ----------

# spark.read.json('dbfs:/mnt/adlsacde/raw/2021-04-18/results.json').createOrReplaceTempView('vwResults_w2')

# COMMAND ----------

# %sql
# select raceid,count(1) from vwResults_w2
# group by raceid
# order by raceid desc limit 10;

# COMMAND ----------

# display(dbutils.fs.mounts())
# display(dbutils.fs.ls("dbfs:/mnt/adlsacde/raw/results.json"))

# display(spark.read.format("json").load("/mnt/adlsacde/raw/results.json"))
# df = spark.read.format("json").option('inferSchema', 'true') .load("/mnt/adlsacde/raw/results.json")

# display(df.printSchema())
# df.schema

from pyspark.sql.types import *

results_schema = StructType([StructField('resultId', IntegerType(), False), 
                             StructField('raceId', IntegerType(), True),
                             StructField('driverId', IntegerType(), True), 
                             StructField('constructorId', IntegerType(), True), 
                             StructField('number', IntegerType(), True), 
                             StructField('grid', IntegerType(), True), 
                             StructField('position', IntegerType(), True), 
                             StructField('positionText', StringType(), True),    
                             StructField('positionOrder', IntegerType(), True), 
                             StructField('points', FloatType(), True), 
                             StructField('laps', IntegerType(), True), 
                             StructField('time', StringType(), True),
                             StructField('milliseconds', IntegerType(), True), 
                             StructField('fastestLap', IntegerType(), True),                              
                             StructField('rank', IntegerType(), True), 
                             StructField('fastestLapTime', StringType(), True), 
                             StructField('fastestLapSpeed', FloatType(), True), 
                             StructField('statusId', StringType(), True), 
                             ])

result_df = spark.read.format("json").schema(results_schema).load(f'{raw_folder_path}/{v_file_date}/results.json')

# display(result_df.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

result_rename_df = result_df.withColumnRenamed('resultId', 'result_id')\
                            .withColumnRenamed('raceId', 'race_id')\
                            .withColumnRenamed('driverId', 'driver_id')\
                            .withColumnRenamed('constructorId', 'constructor_id')\
                            .withColumnRenamed('positionText', 'position_text')\
                            .withColumnRenamed('positionOrder', 'position_order')\
                            .withColumnRenamed('fastestLap', 'fastest_lap')\
                            .withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
                            .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')\
                            .withColumn('ingestion_date', current_timestamp())\
                            .withColumn('data_source', lit(v_data_source))\
                            .withColumn('file_date', lit(v_file_date))

# display(result_rename_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

results_final_df = result_rename_df.drop(col('statusId'))

# display(results_final_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method : 1

# COMMAND ----------

# for result_id_list in results_final_df.select('race_id').distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists('hive_metastore.f1_processed.results') == True):
#         spark.sql(f'alter table hive_metastore.f1_processed.results drop if exists partition (race_id = {result_id_list.race_id})')

# # print(result_id_list)

# COMMAND ----------

# results_final_df.write.mode('overwrite').parquet('/mnt/adlsacde/processed/results')
# results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/adlsacde/processed/results_by_id')
# results_final_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('hive_metastore.f1_processed.results_by_id')

# results_final_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('hive_metastore.f1_processed.results')
# display(spark.read.parquet('/mnt/adlsacde/processed/results').limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method : 2

# COMMAND ----------

# MAGIC %sql
# MAGIC use hive_metastore.f1_processed;

# COMMAND ----------

overwrite_partition(results_final_df, 'hive_metastore', 'f1_processed', 'results', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC race_id,
# MAGIC count(1) 
# MAGIC from hive_metastore.f1_processed.results
# MAGIC group by race_id 
# MAGIC order by race_id desc limit 10;

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists hive_metastore.f1_processed.results;
# MAGIC select * from hive_metastore.f1_processed.results;
