# Databricks notebook source
# MAGIC %md
# MAGIC #Creat Employee Dataframe

# COMMAND ----------

employee_data = [(10,'Vinay', 'Kumar', '1999', '100', 'M', 15000),
                 (20,'Raj', ' Kumar', '2002', '200', 'F', 20000),
                 (30,'Siva', ' Kumar', '2010', '100', None, 10000),
                 (40,'Raja', ' Sing', '2004', '100', 'F', 12000),
                 (50,'Rama', ' Krishnar', '2008', '400', 'M', 18000),
                 (60,'Rasul', 'Kutty', '2014', '500', 'M', 25000),
                 (70,'Kumar', ' Chandra', '2004', '600', 'M', 23000),
                 ]

employee_schema = ['employee_id', 'first_name', 'last_name', 'doj', 'employee_dept_id', 'gender', 'salary']

empDF = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #Creat Department Dataframe

# COMMAND ----------

department_data = [('HR', 100),
                 ('Supply', 200),
                 ('Sales', 300),
                 ('Stock', 400),
                 ]

department_schema = ['dept_name', 'dept_id']

deptDF = spark.createDataFrame(data=department_data, schema=department_schema)

display(deptDF)

# COMMAND ----------

# MAGIC %md
# MAGIC # Inner Join

# COMMAND ----------

from pyspark.sql.functions import col
df_inner = empDF.join(deptDF, empDF.employee_dept_id == deptDF.dept_id, 'inner').withColumn('bonus',col('salary')*0.1).groupBy('dept_name').sum('salary')

display(df_inner)

# COMMAND ----------

df_inner.explain()

# COMMAND ----------

df_inner.explain(extended= True)

# COMMAND ----------

# df_inner.explain(mode='simple')

# df_inner.explain(mode = 'extended')

# df_inner.explain(mode= 'formatted')

# df_inner.explain(mode= 'cost')
