# Databricks notebook source
# DBTITLE 1,Check default Parameters
sc.defaultParallelism

# COMMAND ----------

spark.conf.get('spark.sql.files.maxPartitionBytes')

# COMMAND ----------

# DBTITLE 1,Generating data within spark environment
from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(10), IntegerType())

df.rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,Verify the data within all partitions
df.rdd.glom().collect()

# COMMAND ----------

# DBTITLE 1,Read external file in Spark
dbutils.fs.ls('dbfs:/FileStore/tables/babynames/')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep',',').load('dbfs:/FileStore/tables/babynames/')

df.rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,Change the maxPartitionbytes parameter which changes in no of Partitions
spark.conf.set('spark.sql.files.maxPartitionBytes', '200000')
spark.conf.get('spark.sql.files.maxPartitionBytes')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep',',').load('dbfs:/FileStore/tables/babynames/')

df.rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,Repartition
from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(10), IntegerType())

df.rdd.getNumPartitions()

# COMMAND ----------

df1 = df.repartition(20)
df.rdd.getNumPartitions()

# COMMAND ----------

df1.rdd.glom().collect()

# COMMAND ----------

df1 = df.repartition(2)
df.rdd.getNumPartitions()

# COMMAND ----------

df1.rdd.glom().collect()

# COMMAND ----------

# DBTITLE 1,Coalesce
df2 = df.coalesce(2)
df2.rdd.getNumPartitions()
df2.rdd.glom().collect()
