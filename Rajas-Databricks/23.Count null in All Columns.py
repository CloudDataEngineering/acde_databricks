# Databricks notebook source
# DBTITLE 1,Create Sample Dataframe with Null Values
data_student = [('Vinay', 'Science', 88, 'P', 90),
                 ('Raj', ' Maths', 75,'P', None),
                 ('Siva', ' English', 90, 'P', 80),
                 ('Raja', ' Science',  45, 'F', 75),
                 ('Rama', ' Maths', None, 'F', None),
                 ('Rasul', None, 30, 'F', 50),
                 ('Kumar', ' Social', None, None, 70,),
                 (None,None,None,None,None)
                 ]

schema = ['name', 'subject', 'mark', 'status', 'attendance']

df = spark.createDataFrame(data = data_student, schema = schema)
display(df)


# COMMAND ----------

# DBTITLE 1,Find Null Occurences of Each column in Dataframe
from pyspark.sql.functions import col, count, when

result = df.select([count(when(col(c).isNull(),c)).alias(c) for c in df.columns])

display(result)
