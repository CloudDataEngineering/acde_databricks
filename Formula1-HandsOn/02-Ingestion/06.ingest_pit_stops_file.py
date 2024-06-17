# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

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
                        json('/mnt/adlsacde/raw/pit_stops.json')

display(pit_stops_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

pit_stops_rename_df = pit_stops_df.withColumnRenamed('driverId', 'driver_id').\
                                    withColumnRenamed('raceId', 'race_id').\
                                    withColumn('ingestion_time', current_timestamp())

display(pit_stops_rename_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# display(dbutils.fs.mounts())
# pit_stops_rename_df.write.mode('overwrite').\
#     parquet('/mnt/adlsacde/processed/pitstops')

pit_stops_rename_df.write.mode('overwrite')\
    .format('parquet').saveAsTable('hive_metastore.f1_processed.pitstops')

display(spark.read.parquet('/mnt/adlsacde/processed/pitstops').limit(10))

