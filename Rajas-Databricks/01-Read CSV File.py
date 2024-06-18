# Databricks notebook source
# MAGIC %md
# MAGIC #Create a folder to upload the files

# COMMAND ----------

# %fs mkdirs dbfs:/FileStore/files/babynames
display(dbutils.fs.ls('FileStore/tables/'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Delete folder and uploaded files.

# COMMAND ----------

# %fs rm -r dbfs:/FileStore/files/babynames

# COMMAND ----------

# MAGIC %md
# MAGIC #Uploaded files list

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/files/babynames')

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Singel CSV file

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header',True).option('sep',',').load('dbfs:/FileStore/files/babynames/baby_names_1960to1965.csv')

display(df)

print(df.count())

# COMMAND ----------

df = spark.read.format('csv').options(inferSchema='True', header='True', sep=',').load('dbfs:/FileStore/files/babynames/baby_names_1960to1965.csv')

display(df)

print(df.count())

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header', True).option('sep',',').load(['dbfs:/FileStore/files/babynames/baby_names_1960to1965.csv','dbfs:/FileStore/files/babynames/baby_names_1966to1970.csv'])

display(df)

print(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Read all CSV files under a folder.

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep', ',').load('dbfs:/FileStore/files/babynames/*')

display(df)

print(df.count())

# COMMAND ----------

# Print the schema

print(df.schema)
# df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Own Schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_defined = StructType([StructField('Year_of_Birth', IntegerType(), True), 
                             StructField('First_Name', StringType(), True), 
                             StructField('Sex', StringType(), True), 
                             StructField('Country', StringType(), True), 
                             StructField('Count', StringType(), True)
                             ])

# COMMAND ----------

df = spark.read.format('csv').schema(schema_defined).option('header', True).option('sep', ',').load('dbfs:/FileStore/files/babynames/*')

display(df)

print(df.count())

# COMMAND ----------

schema_alternate = 'Year_of_Birth INTEGER, First_Name STRING, Sex STRING, Country STRING, Count INTEGER'

# COMMAND ----------

df = spark.read.format('csv').schema(schema_alternate).option('header', True).option('sep',',').load('dbfs:/FileStore/files/babynames/*')

display(df)

# COMMAND ----------

# print(df.schema)
df.printSchema()
