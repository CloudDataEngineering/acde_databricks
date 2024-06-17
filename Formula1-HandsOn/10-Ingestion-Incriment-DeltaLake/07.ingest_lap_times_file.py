# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

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
# display(dbutils.fs.ls("dbfs:/mnt/adlsacde/raw/lap_times/"))

# display(spark.read.csv('dbfs:/mnt/adlsacde/raw/lap_times/lap_times_split_1.csv'))

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

lap_times_schema = StructType([StructField("raceId", IntegerType(), False),
                               StructField("driverId", IntegerType(), True),
                               StructField("lap", IntegerType(), True),
                               StructField("position", IntegerType(), True),
                               StructField("time", StringType(), True),
                               StructField("milliseconds", IntegerType(), True)
                                ])

lap_times_df = spark.read.schema(lap_times_schema).\
                csv(f'{raw_folder_path}/{v_file_date}/lap_times/*')

# display(lap_times_df.limit(10))
display(lap_times_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

final_df = lap_times_df.withColumnRenamed("raceId", "race_id").\
                            withColumnRenamed("driverId", "driver_id").\
                            withColumn('ingestion_time', current_timestamp())

display(final_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# display(dbutils.fs.mounts())

# final_df.write.mode('overwrite').parquet('/mnt/adlsacde/processed/laptimes')

# final_df.write.mode('overwrite').format('parquet').saveAsTable('hive_metastore.f1_processed.laptimes')

# # display(spark.read.parquet('/mnt/adlsacde/processed/laptimes').limit(10))
# display(spark.read.parquet('/mnt/adlsacde/processed/laptimes').count())

# COMMAND ----------

# MAGIC %sql
# MAGIC use hive_metastore.f1_processed;

# COMMAND ----------

# overwrite_partition(final_df, 'hive_metastore', 'f1_processed', 'laptimes', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id and tgt.lap = src.lap and tgt.race_id = src.race_id"
# merge_delta_data(input_df, schema_name, db_name, table_name, folder_path, merge_condition, partition_column)
merge_delta_data(final_df, 'hive_metastore', 'f1_processed', 'laptimes', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC race_id,
# MAGIC count(1) 
# MAGIC from hive_metastore.f1_processed.laptimes
# MAGIC group by race_id 
# MAGIC order by race_id desc limit 10;

# COMMAND ----------

dbutils.notebook.exit('Success')
