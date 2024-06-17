# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source= dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %run "../03-Includes/02-common_functions"

# COMMAND ----------

# display(dbutils.fs.mounts())
# display(dbutils.fs.ls('/mnt/adlsacde/raw'))

# display(spark.read.option('multiline', 'true'). \
        # json('/mnt/adlsacde/raw/pit_stops.json'))

# df = spark.read.option('multiline', 'true'). \
#     json('/mnt/adlsacde/raw/pit_stops.json')

# df.schema

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

pit_stops_schema = StructType([StructField('raceId', IntegerType(), False), 
                               StructField('driverId', IntegerType(), True), 
                               StructField('stop', StringType(), True), 
                               StructField('lap', IntegerType(), True), 
                               StructField('time', StringType(), True),
                               StructField('duration', StringType(), True), 
                               StructField('milliseconds', IntegerType(), True)
                               ])

pit_stops_df = spark.read.schema(pit_stops_schema).\
                        option('multiline', 'true'). \
                        json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# display(pit_stops_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

results_final_df = pit_stops_df.withColumnRenamed('driverId', 'driver_id').\
                                    withColumnRenamed('raceId', 'race_id').\
                                    withColumn('ingestion_time', current_timestamp())

# display(results_final_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# # display(dbutils.fs.mounts())
# # results_final_df.write.mode('overwrite').\
# #     parquet('/mnt/adlsacde/processed/pitstops')

# results_final_df.write.mode('overwrite')\
#     .format('parquet').saveAsTable('hive_metastore.f1_processed.pitstops')

# display(spark.read.parquet('/mnt/adlsacde/processed/pitstops').limit(10))


# COMMAND ----------

# MAGIC %sql
# MAGIC use hive_metastore.f1_processed;

# COMMAND ----------

overwrite_partition(results_final_df, 'hive_metastore', 'f1_processed', 'pitstops', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC race_id,
# MAGIC count(1) 
# MAGIC from hive_metastore.f1_processed.pitstops
# MAGIC group by race_id 
# MAGIC order by race_id desc limit 10;

# COMMAND ----------

dbutils.notebook.exit('Success')
