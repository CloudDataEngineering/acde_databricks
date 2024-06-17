# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

# display(dbutils.fs.mounts())
# display(dbutils.fs.ls("/mnt/adlsacde/raw"))
# display(dbutils.fs.ls("/mnt/adlsacde/raw/constructors.json"))

constructor_schema = 'constructorId INT, constructorRef STRING ,name STRING, nationality STRING, url STRING'

constructor_df = spark.read.json("/mnt/adlsacde/raw/constructors.json", schema=constructor_schema)

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
display(dbutils.fs.ls('/mnt/adlsacde/processed'))

# COMMAND ----------

# constructor_final_df.write.mode('overwrite').parquet('/mnt/adlsacde/processed/constructor')

constructor_final_df.write.mode('overwrite').format('parquet').saveAsTable('hive_metastore.f1_processed.constructor')

# display(dbutils.fs.ls('/mnt/adlsacde/processed/constructor'))


# COMMAND ----------

display(spark.read.parquet('/mnt/adlsacde/processed/constructor'))
