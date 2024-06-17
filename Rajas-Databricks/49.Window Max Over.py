# Databricks notebook source
# MAGIC %md
# MAGIC ## Scenario
# MAGIC
# MAGIC ### Lets assume we have duplicate records in our dataset based on particular key. We need to remove duplicate records but at the same time need to consider the maximum value of each column among duplicate values. To develop a logic for this requirement, below are the steps.

# COMMAND ----------

# DBTITLE 1,Sample Data
sampleData = ((100, 'Mobile', 5000, 10), \
               (100, 'Mobile', 7000, 7),
               (200, 'Laptop', 20000, 4),
               (200, 'Laptop', 25000, 8),
               (200, 'Laptop', 22000, 12)
               )

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

defSchema = StructType([ \
    StructField('Product_id', IntegerType(), True), 
    StructField('Product_name', StringType(), True), 
    StructField('Price', IntegerType(), True), 
    StructField('DiscountPercent', IntegerType(), True), 
])

df = spark.createDataFrame(data = sampleData, schema = defSchema)
display(df)

# COMMAND ----------

# DBTITLE 1,Max Over window function
from pyspark.sql import Window
from pyspark.sql.functions import max, col

windowSpec = Window.partitionBy('Product_id')

dfMax = (df.withColumn('maxPrice', max('Price').over(windowSpec))
         .withColumn('maxDiscountPercent', max('DiscountPercent').over(windowSpec)))
display(dfMax)

# COMMAND ----------

# DBTITLE 1,Select Max Columns
dfSel = dfMax.select(col('Product_id'),col('Product_name'),col('maxPrice').alias('Price'),col('maxDiscountPercent').alias('DiscountPercent'))

display(dfSel)

# COMMAND ----------

# DBTITLE 1,Remove Duplicates
dfOut = dfSel.drop_duplicates()
display(dfOut)

# COMMAND ----------

# DBTITLE 1,Complete Code
from pyspark.sql import Window
from pyspark.sql.functions import max, col

windowSpec = Window.partitionBy('Product_id')

dfMax = (df.withColumn('maxPrice', max('Price').over(windowSpec))
         .withColumn('maxDiscountPercent', max('DiscountPercent').over(windowSpec)))
# display(dfMax)

dfSel = dfMax.select(col('Product_id'),col('Product_name'),col('maxPrice').alias('Price'),col('maxDiscountPercent').alias('DiscountPercent'))

# display(dfSel)

dfOut = dfSel.drop_duplicates()
display(dfOut)
