# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %run "../03-Includes/02-common_functions"

# COMMAND ----------

# display(dbutils.fs.mounts())
# display(dbutils.fs.ls('/mnt/acdeadls/raw'))

# COMMAND ----------

# import pandas as pd
# from pyspark.sql import SparkSession

# # Create SparkSession
# spark = SparkSession.builder.getOrCreate()

# # Read the pandas DataFrame
# df_races = pd.read_csv('https://raw.githubusercontent.com/CloudDataEngineering/acde_databricks/main/raw/files/formula1/races.csv')

# # Convert pandas DataFrame to Spark DataFrame
# spark_df_races = spark.createDataFrame(df_races)

# # Write Spark DataFrame to CSV
# spark_df_races.write.mode('overwrite').option('header', 'true').option('inferSchema', 'true').format('csv').save(f'{raw_folder_path}/races.csv')

# # Read the saved CSV file as a Spark DataFrame
# spark_df = spark.read.csv(f'{raw_folder_path}/races.csv', header=True)

# # Display the Spark DataFrame
# # display(spark_df).limit(10)

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
    .csv("/mnt/acdeadls/raw/races.csv")

# display(races_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# display(races_with_timestamp_df.limit(10))

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

# display(races_selected_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format

# COMMAND ----------

races_selected_df.write.mode('overwrite').format('parquet').saveAsTable('hive_metastore.f1_processed.races')

# hive_metastore.f1_processed

# COMMAND ----------

# display(spark.read.parquet('/mnt/acdeadls/processed/races').limit(10))

# COMMAND ----------

# races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/acdeadls/processed/races_by_year')

races_selected_df.write.mode('overwrite').partitionBy('race_year').format('parquet').saveAsTable('hive_metastore.f1_processed.races_by_year')


# display(spark.read.parquet('/mnt/acdeadls/processed/races_by_year').limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended hive_metastore.f1_processed.races_by_year
