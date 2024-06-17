# Databricks notebook source
# DBTITLE 1,Create Sample Dataframe
Transaction = [(100, 'Cosmetic', 150),
               (200, 'Apparel', 250),
               (300, 'Shirt', 400)]
transactionDF = spark.createDataFrame(data = Transaction, schema = ['Store_Id', 'Item', 'Amount'])

display(transactionDF)

# COMMAND ----------

# DBTITLE 1,Create sample Dimension Table
Store = [
    (100, 'Store_London'),
     (200, 'Store_India'),
     (300, 'Store_Dubai')
]

storeDF = spark.createDataFrame(data = Store, schema = ['Store_Id', 'Store_name'])
display(storeDF)

# COMMAND ----------

from pyspark.sql.functions import broadcast

joinDF = transactionDF.join(broadcast(storeDF),transactionDF['Store_Id'] == storeDF['Store_Id'])

joinDF.show()

# COMMAND ----------

joinDF.explain()
