# Databricks notebook source
# MAGIC %md
# MAGIC #Pyspark Corrupt Records Mode

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sample Dataframe

# COMMAND ----------

df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('dbfs:/FileStore/tables/Production_data_corrupt.csv')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schema

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField('Month', StringType(), True),
    StructField('Emp_count', IntegerType(), True),
    StructField('Production_unit', IntegerType(), True),
    StructField('Expense', IntegerType(), True),
    StructField('_corrupt_record', StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permissive Mode

# COMMAND ----------

df = spark.read.format('csv') \
    .option('mode', 'PERMISSIVE') \
    .option('header', 'true') \
    .schema(schema) \
    .load('dbfs:/FileStore/tables/Production_data_corrupt.csv')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop malformed

# COMMAND ----------

df = spark.read.format('csv') \
    .option('mode', 'DROPMALFORMED') \
    .option('header', 'true') \
    .schema(schema) \
    .load('dbfs:/FileStore/tables/Production_data_corrupt.csv')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FailFast Mode

# COMMAND ----------

df = spark.read.format('csv') \
    .option('mode', 'FAILFAST') \
    .option('header', 'true') \
    .schema(schema) \
    .load('dbfs:/FileStore/tables/Production_data_corrupt.csv')

display(df)
