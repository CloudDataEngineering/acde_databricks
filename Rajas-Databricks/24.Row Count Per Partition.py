# Databricks notebook source
df = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep', ',').load('dbfs:/FileStore/tables/baby_names_by_state.csv')

display(df)

# COMMAND ----------

# DBTITLE 1,Number of Records in the Dataframe
print(df.count())

# COMMAND ----------

# DBTITLE 1,Default Partition Count
print(df.rdd.getNumPartitions())

# COMMAND ----------

# DBTITLE 1,Number of Records per Partition
from pyspark.sql.functions import spark_partition_id
df.withColumn('partitionId', spark_partition_id()).groupBy('partitionId').count().show()

# COMMAND ----------

# DBTITLE 1,Repartition the Dataframe to 5
df_5 = df.select(df.Year,df.Count,df.Sex,df.Count).repartition(10)

# COMMAND ----------

# DBTITLE 1,Get Number of Partitions in the Dataframe
print(df_5.rdd.getNumPartitions())

# COMMAND ----------

# DBTITLE 1,Get Number of Records per Partitions
df_5.withColumn('partitionId', spark_partition_id()).groupBy('partitionId').count().show()

# COMMAND ----------

# DBTITLE 1,Spark Partition ID
df2 = df.repartition(20).withColumn('partitionId',spark_partition_id())
df2.display()

# COMMAND ----------

# DBTITLE 1,Check Data Skewness in the dataframe
from pyspark.sql.functions import spark_partition_id, asc, desc

df3 = df2\
    .withColumn('partitionId', spark_partition_id())\
    .groupBy('partitionId')\
    .count()\
    .orderBy(desc('count'))

display(df3)
