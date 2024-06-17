# Databricks notebook source
# MAGIC %md
# MAGIC ## Pyspark Function Join

# COMMAND ----------

# DBTITLE 1,Create Sameple Dataframe
employee_data = [(10,'Vinay Kumar', '1999-09-01', '100', 'M', 15000),
                 (20,'Raj Kumar', '2002-11-01', '200', 'F', 20000),
                 (30,'Siva Kumar', '2010-01-01', '100', 'M', 10000),
                 (40,'Raja Sing', '2004-07-12', '100', 'F', 12000),
                 (50,'Rama Krishnar', '2008-08-01', '400', 'M', 18000),
                 (60,'Rasul Kutty', '2014-05-01', '500', 'M', 25000),
                 (70,'Kumar Chandra', '2004-11-01', '600', 'M', 23000),
                 ]

employee_schema = ['employee_id', 'Name', 'doj', 'employee_dept_id', 'gender', 'salary']

empDF = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(empDF)

# COMMAND ----------

# DBTITLE 1,Create iterable data
column_list = empDF.columns

print(column_list)

# COMMAND ----------

# DBTITLE 1,Join the string
joined_string = ','.join(column_list)

print(joined_string)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Cases

# COMMAND ----------

# DBTITLE 1,generate table Join function
joinstring = ' AND '.join(list(map(lambda x: ('Target.' + x + ' = Source.' + x), column_list)))

display(joinstring)

# COMMAND ----------

# DBTITLE 1,generate Update functon
updatestring = ','.join(list(map(lambda x: ('Target.' + x + ' = Source.' + x), column_list)))

display(updatestring)

# COMMAND ----------

# DBTITLE 1,generate source insert function
sourceinsertstring = ','.join(list(map(lambda x: ('Source.' + x), column_list)))

display(sourceinsertstring)
