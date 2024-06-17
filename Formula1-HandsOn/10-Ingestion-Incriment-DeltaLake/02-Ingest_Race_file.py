# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %run "../03-Includes/02-common_functions"

# COMMAND ----------

# display(dbutils.fs.mounts())
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
    .csv(f'{raw_folder_path}/{v_file_date}/races.csv')
    # .csv("/mnt/adlsacde/raw/races.csv")

display(races_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                                  .withColumn('data_source', lit(v_data_source)) \
                                  .withColumn('file_date', lit(v_file_date))

display(races_with_timestamp_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the columns required & rename as required

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df = races_with_timestamp_df.select(
    col('raceId').alias('race_id'),
    col('year').alias('race_year'),
    col('round'),
    col('circuitId').alias('circuit_id'),
    col('name'),
    col('race_timestamp'),
    col('ingestion_date'),
    col('data_source'),
    col('file_date')
)

display(races_selected_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format

# COMMAND ----------

# DBTITLE 1,Write Races Selected Parquet Table
# races_selected_df.write.mode('overwrite').format('parquet').saveAsTable('hive_metastore.f1_processed.races')

# COMMAND ----------

# DBTITLE 1,Overwrite Races Selected Dataframe as Delta Table
races_selected_df.write.mode('overwrite').format('delta').saveAsTable('hive_metastore.f1_processed.races')

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('hive_metastore.f1_processed.races_by_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_processed.races limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_processed.races_by_year limit 10;

# COMMAND ----------

# display(spark.read.parquet('/mnt/adlsacde/processed/races').limit(10))

# COMMAND ----------

# races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/adlsacde/processed/races_by_year')

# races_selected_df.write.mode('overwrite').partitionBy('race_year').format('parquet').saveAsTable('hive_metastore.f1_processed.races_by_year')


# display(spark.read.parquet('/mnt/adlsacde/processed/races_by_year').limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended hive_metastore.f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit('OK')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC race_year,
# MAGIC count(1) 
# MAGIC from hive_metastore.f1_processed.races
# MAGIC group by race_year 
# MAGIC order by race_year desc limit 10;
