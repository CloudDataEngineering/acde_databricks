# Databricks notebook source
# DBTITLE 1,Read CSV File without Any option
df_no_option = spark.read.format('csv').load('dbfs:/FileStore/tables/babynames/babynames_input')

# COMMAND ----------

display(df_no_option)

# COMMAND ----------

# DBTITLE 1,Read CSV File with InferSchema option
df = spark.read.format('csv').option('inferSchema', True).load('dbfs:/FileStore/tables/babynames/babynames_input')

display(df)

# COMMAND ----------

# DBTITLE 1,Read CSV File with Defined Schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_defined = StructType([StructField('Year_of_Birth', IntegerType(), True), 
                             StructField('First_Name', StringType(), True), 
                             StructField('Sex', StringType(), True), 
                             StructField('Country', StringType(), True), 
                             StructField('Count', StringType(), True)
                             ])

# COMMAND ----------

df = spark.read.format('csv').schema(schema_defined).option('header', True).option('sep', ',').load('dbfs:/FileStore/babynames/*')
