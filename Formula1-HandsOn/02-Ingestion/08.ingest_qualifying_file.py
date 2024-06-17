# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json files

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
                schema(qualifying_schema).json('dbfs:/mnt/adlsacde/raw/qualifying/*').\
                withColumnRenamed('constructorId', 'constructor_id').\
                withColumnRenamed('driverId', 'driver_id').\
                withColumnRenamed('qualifyId', 'qualifying_id').\
                withColumnRenamed('raceId', 'race_id').\
                withColumn('ingestion_time', current_timestamp())

# qualifying_df.write.mode('append').parquet('/mnt/adlsacde/processed/qualifying')

qualifying_df.write.mode('append').format('parquet').saveAsTable('hive_metastore.f1_processed.qualifying')

display(spark.read.parquet('/mnt/adlsacde/processed/qualifying').limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format
