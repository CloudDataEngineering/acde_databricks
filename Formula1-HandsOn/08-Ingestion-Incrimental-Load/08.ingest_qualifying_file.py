# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json files

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

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# display(dbutils.fs.mounts())
# display(dbutils.fs.ls('dbfs:/mnt/adlsacde/raw/qualifying/'))

# df = spark.read.option('multiline', 'true').json('dbfs:/mnt/adlsacde/raw/qualifying/qualifying_split_1.json')
# display(df.limit(10))

# display(df.schema)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import current_timestamp

qualifying_schema = StructType([StructField('constructorId', IntegerType(), False), 
                                StructField('driverId', IntegerType(), True), 
                                StructField('number', IntegerType(), True), 
                                StructField('position', IntegerType(), True), 
                                StructField('q1', StringType(), True), 
                                StructField('q2', StringType(), True), 
                                StructField('q3', StringType(), True), 
                                StructField('qualifyId', IntegerType(), True), 
                                StructField('raceId', IntegerType(), True)])

qualifying_df = spark.read.\
                option('multiline', 'true').\
                schema(qualifying_schema).json(f'{raw_folder_path}/{v_file_date}/qualifying/*').\
                withColumnRenamed('constructorId', 'constructor_id').\
                withColumnRenamed('driverId', 'driver_id').\
                withColumnRenamed('qualifyId', 'qualifying_id').\
                withColumnRenamed('raceId', 'race_id').\
                withColumn('ingestion_time', current_timestamp())

results_final_df = qualifying_df

# qualifying_df.write.mode('append').parquet('/mnt/adlsacde/processed/qualifying')

# qualifying_df.write.mode('append').format('parquet').saveAsTable('hive_metastore.f1_processed.qualifying')

# display(spark.read.parquet(f'{processed_folder_path}/qualifying').limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %sql
# MAGIC use hive_metastore.f1_processed;

# COMMAND ----------

overwrite_partition(results_final_df, 'hive_metastore', 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC race_id,
# MAGIC count(1) 
# MAGIC from hive_metastore.f1_processed.qualifying
# MAGIC group by race_id 
# MAGIC order by race_id desc limit 10;
