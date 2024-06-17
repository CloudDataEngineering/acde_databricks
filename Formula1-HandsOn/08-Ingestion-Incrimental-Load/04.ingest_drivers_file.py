# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

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
# display(dbutils.fs.ls("/mnt/adlsacde/raw/drivers.json"))
# display(spark.read.json("/mnt/adlsacde/raw/drivers.json"))
# df_json = spark.read.json("/mnt/adlsacde/raw/drivers.json")
# df_json.printSchema()
# df_json.schema

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, DateType

name_schema = StructType([StructField('forename', StringType(), True), 
            StructField('surname', StringType(), True)])

driver_schema = StructType([StructField('driverId', IntegerType(), True),
                            StructField('driverRef', StringType(), True), 
                            StructField('number', IntegerType(), True),
                            StructField('code', StringType(), True), 
                            StructField('name', name_schema),
                            StructField('dob', DateType(), True), 
                            StructField('nationality', StringType(), True), 
                            StructField('url', StringType(), True)])

driver_df = spark.read.\
    schema(driver_schema).\
    json(f'{raw_folder_path}/{v_file_date}/drivers.json')

display(driver_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id  
# MAGIC 1. driverRef renamed to driver_ref  
# MAGIC 1. ingestion date added
# MAGIC 1. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col,concat

driver_rename_df = driver_df.withColumnRenamed('driverId', 'driver_id').\
                            withColumnRenamed('driverRef', 'driver_ref').\
                            withColumn('ingestion_date', current_timestamp()).\
                            withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))

display(driver_rename_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 1. name.surname
# MAGIC 1. url

# COMMAND ----------

driver_final_df = driver_rename_df.drop('url').\
                            drop('name.forename').\
                            drop('name.surname')

display(driver_final_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# display(dbutils.fs.mounts())
driver_final_df.write.mode('overwrite').format('parquet').saveAsTable('hive_metastore.f1_processed.driver')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/driver'))

# COMMAND ----------

dbutils.notebook.exit('Success')
