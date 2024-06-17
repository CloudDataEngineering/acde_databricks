# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

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

result_df = spark.read.format("json").schema(results_schema).load("/mnt/adlsacde/raw/results.json")

display(result_df.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

result_rename_df = result_df.withColumnRenamed('resultId', 'result_id').\
                            withColumnRenamed('raceId', 'race_id').\
                            withColumnRenamed('driverId', 'driver_id').\
                            withColumnRenamed('constructorId', 'constructor_id').\
                            withColumnRenamed('positionText', 'position_text').\
                            withColumnRenamed('positionOrder', 'position_order').\
                            withColumnRenamed('fastestLap', 'fastest_lap').\
                            withColumnRenamed('fastestLapTime', 'fastest_lap_time').\
                            withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed').\
                            withColumn('ingestion_date', current_timestamp())

display(result_rename_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

results_final_df = result_rename_df.drop(col('statusId'))

display(results_final_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# results_final_df.write.mode('overwrite').parquet('/mnt/adlsacde/processed/results')

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/adlsacde/processed/results_by_id')

display(spark.read.parquet('/mnt/adlsacde/processed/results_by_id').limit(10))
