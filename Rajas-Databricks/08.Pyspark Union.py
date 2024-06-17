# Databricks notebook source
# MAGIC %md
# MAGIC ##Spark Version

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').getOrCreate()
print(spark.sparkContext.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create dataframe DF1

# COMMAND ----------

employee_data = [(10,'Vinay', 'Kumar', '1999', '100', 'M', 15000),
                 (20,'Raj', ' Kumar', '2002', '200', 'F', 20000),
                 (30,'Siva', ' Kumar', '2010', '100', None, 10000),
                 ]

employee_schema = ['employee_id', 'first_name', 'last_name', 'doj', 'employee_dept_id', 'gender', 'salary']

df1 = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create dataframe DF2

# COMMAND ----------

employee_data = [(30,'Siva', ' Kumar', '2010', '100', None, 10000),
                 (40,'Raja', ' Sing', '2004', '100', 'F', 12000),
                 (50,'Rama', ' Krishnar', '2008', '400', 'M', 18000),
                 ]

employee_schema = ['employee_id', 'first_name', 'last_name', 'doj', 'employee_dept_id', 'gender', 'salary']

df2 = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create dataframe DF3

# COMMAND ----------

employee_data = [(50,'Rama', ' Krishnar', '2008', '400', 'M', 18000),
                 (60,'Rasul', 'Kutty', '2014', '500', 'M', 25000),
                 (70,'Kumar', ' Chandra', '2004', '600', 'M', 23000), 
                 ]

employee_schema = ['employee_id', 'first_name', 'last_name', 'doj', 'employee_dept_id', 'gender', 'salary']

df3 = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(df3)               

# COMMAND ----------

# MAGIC %md
# MAGIC ##Union

# COMMAND ----------

df4 = df1.union(df2)

display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop Duplicates

# COMMAND ----------

display(df4.dropDuplicates())

# COMMAND ----------

# MAGIC %md
# MAGIC ##UnionAll

# COMMAND ----------

df5 = df1.union(df2)

display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Schema Mismatch

# COMMAND ----------

df6 = df2.select(df2.employee_id,df2.first_name,df2.last_name,df2.doj,df2.employee_dept_id,
                 df2.gender)

display(df6)

# COMMAND ----------

df_invalid = df1.union(df6)
