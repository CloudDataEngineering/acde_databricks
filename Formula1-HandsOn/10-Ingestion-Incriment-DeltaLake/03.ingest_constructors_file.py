# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

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
# display(dbutils.fs.ls("/mnt/adlsacde/raw"))
# display(dbutils.fs.ls("/mnt/adlsacde/raw/constructors.json"))

constructor_schema = 'constructorId INT, constructorRef STRING ,name STRING, nationality STRING, url STRING'

constructor_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/constructors.json', schema=constructor_schema)

display(constructor_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

constructor_drop_df = constructor_df.drop(col('url'))

display(constructor_drop_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructor_final_df = constructor_drop_df.withColumnRenamed('constructorId', 'constructor_id').\
                                            withColumnRenamed('constructorRef', 'constructor_ref').\
                                            withColumn('intigratin_date', current_timestamp())

display(constructor_final_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file

# COMMAND ----------

# display(dbutils.fs.mounts())
# display(dbutils.fs.ls('/mnt/adlsacde/processed'))

# COMMAND ----------

# constructor_final_df.write.mode('overwrite').parquet('/mnt/adlsacde/processed/constructor')

# constructor_final_df.write.mode('overwrite').format('parquet').saveAsTable('hive_metastore.f1_processed.constructor')

# display(dbutils.fs.ls('/mnt/adlsacde/processed/constructor'))


# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('delta').saveAsTable('hive_metastore.f1_processed.constructor')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_processed.constructor limit 10;

# COMMAND ----------

# display(spark.read.parquet(f'{processed_folder_path}/constructor'))

# COMMAND ----------

dbutils.notebook.exit('Success')
