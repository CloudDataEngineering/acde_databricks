# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

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
                csv("dbfs:/mnt/adlsacde/raw/lap_times/*")

# display(lap_times_df.limit(10))
display(lap_times_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

lap_final_df = lap_times_df.withColumnRenamed("raceId", "race_id").\
                            withColumnRenamed("driverId", "driver_id").\
                            withColumn('ingestion_time', current_timestamp())

display(lap_final_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# display(dbutils.fs.mounts())

lap_final_df.write.mode('overwrite').parquet('/mnt/adlsacde/processed/laptimes')

# display(spark.read.parquet('/mnt/adlsacde/processed/laptimes').limit(10))
display(spark.read.parquet('/mnt/adlsacde/processed/laptimes').count())
