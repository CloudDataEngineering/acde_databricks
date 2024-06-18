# Databricks notebook source
dbutils.fs.ls('FileStore/tables')

# COMMAND ----------

# Read the CSV file into a DataFrame and specify that the first row is the header
baby_names = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep',',').load('dbfs:/FileStore/files/babynames/baby_names.csv')

# COMMAND ----------

display(baby_names)

# COMMAND ----------

display(baby_names.schema)

# COMMAND ----------

from pyspark.sql.types import StructField,StringType,IntegerType,StructType

baby_names_schema = StructType([StructField('Year', IntegerType(), True), 
                                StructField('First Name', StringType(), True), 
                                StructField('Sex', StringType(), True), 
                                StructField('Country', StringType(), True), 
                                StructField('Count', IntegerType(), True)
                                ])

# COMMAND ----------

baby_names = spark.read.format('csv').schema(baby_names_schema).option('inferSchema', True).option('header', True).option('sep', ',').load('dbfs:/FileStore/files/babynames/baby_names.csv')
display(baby_names)

# COMMAND ----------

display(baby_names.schema)

# COMMAND ----------

# display(baby_names.filter(baby_names.Year == '1960').count())
baby_names.write.format('delta').saveAsTable('baby_names')
# baby_names =  baby_names.withColumnRenamed('First Name','Name')
# display(baby_names)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table baby_names_1960to1965;
# MAGIC -- select* from baby_names_1960to1965
