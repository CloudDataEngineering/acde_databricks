# Databricks notebook source
# MAGIC %md
# MAGIC ##Define Schema for our sample streaming data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_defined = StructType([StructField('File', StringType(), True),
                             StructField('Shop', StringType(), True),
                             StructField('Sale_count', IntegerType(), True)
                             ])

# COMMAND ----------

# DBTITLE 1,Create the folder Structure in DBFS file System
# dbutils.fs.rm('/FileStore/tables/stream_checkpoint/', True)
# dbutils.fs.rm('/FileStore/tables/stream_read/', True)
# dbutils.fs.rm('/FileStore/tables/stream_write/', True)

dbutils.fs.mkdirs('/FileStore/tables/stream_checkpoint/')
dbutils.fs.mkdirs('/FileStore/tables/stream_read/')
dbutils.fs.mkdirs('/FileStore/tables/stream_write/')

# COMMAND ----------

# DBTITLE 1,Read Stream data
df = spark.readStream.format('csv').schema(schema_defined).option('header',True).option('sep',',').load('dbfs:/FileStore/tables/stream_read/')

df1 = df.groupBy('Shop').sum('Sale_count')
display(df1)

# COMMAND ----------

# DBTITLE 1,Write Streaming data
df2=df.writeStream.format('parquet').outputMode('append').option('path','/FileStore/tables/stream_write/').option('checkpointLocation','/FileStore/tables/stream_checkpoint/').start().awaitTermination()

# COMMAND ----------

# DBTITLE 1,Verify the written Stream output data
display(spark.read.format('parquet').load('/FileStore/tables/stream_write/*.parquet'))
