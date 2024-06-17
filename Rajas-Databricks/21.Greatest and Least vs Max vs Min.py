# Databricks notebook source
# DBTITLE 1,Create Sample Dataframe
input_data = [('Vinay', 70,68,88,46,69),
              ('Siva', 90,58,88,56,89),
              ('Roopa', 80,78,38,66,59),
              ('Hony', 60,58,98,66,49),
              ('MMM', 76,98,48,46,89)
              ]

schema = ['Student', 'Subject_1', 'Subject_2', 'Subject_3', 'Subject_4', 'Subject_5']

df = spark.createDataFrame(input_data, schema)
df.display()
# display(df)
# df.show()

# COMMAND ----------

# DBTITLE 1,Greatest of Columns
from pyspark.sql.functions import greatest

greatDF = df.withColumn('Greatest', greatest('Subject_1', 'Subject_2', 'Subject_3', 'Subject_4', 'Subject_5'))

greatDF.display()

# COMMAND ----------

# DBTITLE 1,Least of Columns
from pyspark.sql.functions import least

greatDF = df.withColumn('Least', least('Subject_1', 'Subject_2', 'Subject_3', 'Subject_4', 'Subject_5'))

greatDF.display()

# COMMAND ----------

# DBTITLE 1,Max of Row
# df.agg({'Subject_1': 'max'}).display()

df.agg({'Subject_1': 'max', 'Subject_2': 'max','Subject_3': 'max','Subject_4': 'max','Subject_5': 'max'}).display()

# COMMAND ----------

# DBTITLE 1,Min of Row
df.agg({'Subject_1': 'min', 'Subject_2': 'min','Subject_3': 'min','Subject_4': 'min','Subject_5': 'min'}).display()
