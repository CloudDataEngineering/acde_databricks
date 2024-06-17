# Databricks notebook source
# DBTITLE 1,Sample Data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

data = ((100, 'Mobile', 20000, 10),
               (200, 'Television', 85000, 12),
               (300, 'Laptop', 45000, 4),
               (400, 'Monitor', 5000, 8),
               (500, 'Headset', 65000, 12)
               )

schema = StructType([ \
    StructField('Product_id', IntegerType(), True), 
    StructField('Product_name', StringType(), True), 
    StructField('UnitPrice', IntegerType(), True), 
    StructField('DiscountPercent', IntegerType(), True), 
])

df = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

# DBTITLE 1,CREATE_MAP - Convert Columns to Dictionary
from pyspark.sql.functions import col, lit, create_map

dfDict = df.select(col('Product_id'),col('Product_name'),col('UnitPrice'),col('DiscountPercent'),create_map(col('Product_name'),col('UnitPrice')).alias('PriceDict'))
display(dfDict)

# COMMAND ----------

from pyspark.sql.functions import col, lit, create_map

dfDict = df.withColumn('PriceDict',create_map(lit('Product_name'),col('Product_name'),lit('UnitPrice'),col('UnitPrice')))

display(dfDict)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

dfDict.printSchema()
