# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

display(dbutils.fs.mounts())
# display(dbutils.fs.ls('/mnt/adlsacde/raw'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv("/mnt/adlsacde/raw/races.csv")

display(races_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

display(races_with_timestamp_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the columns required & rename as required

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), \
                                                col('year').alias('race_year'), \
                                                col('round'),\
                                                col('circuitId').alias('circuit_id'), \
                                                col('name'), \
                                                col('race_timestamp'), \
                                                col('ingestion_date'))

display(races_selected_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format

# COMMAND ----------

races_selected_df.write.mode('overwrite').format('parquet').saveAsTable('hive_metastore.f1_processed.races')

# hive_metastore.f1_processed

# COMMAND ----------

display(spark.read.parquet('/mnt/adlsacde/processed/races').limit(10))

# COMMAND ----------

# races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/adlsacde/processed/races_by_year')

races_selected_df.write.mode('overwrite').partitionBy('race_year').format('parquet').saveAsTable('hive_metastore.f1_processed.races_by_year')


display(spark.read.parquet('/mnt/adlsacde/processed/races_by_year').limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended hive_metastore.f1_processed.races_by_year
