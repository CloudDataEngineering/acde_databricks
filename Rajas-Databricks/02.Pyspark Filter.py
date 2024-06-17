# Databricks notebook source
# MAGIC %md
# MAGIC # Pyspark Filter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dataframe

# COMMAND ----------

employee_data = [(10,'Vinay Kumar', '1999', '100', 'M', 15000),
                 (20,'Raj Kumar', '2002', '200', 'F', 20000),
                 (30,'Siva Kumar', '2010', '100', None, 10000),
                 (40,'Raja Sing', '2004', '100', 'F', 12000),
                 (50,'Rama Krishnar', '2008', '400', 'M', 18000),
                 (60,'Rasul', '2014', '500', 'M', 25000),
                 (70,'Kumar Chandra', '2004', '600', 'M', 23000),
                 ]

employee_schema = ['employee_id', 'name', 'doj', 'employee_dept_id', 'gender', 'salary']

employeeDF = spark.createDataFrame(data=employee_data, schema=employee_schema)

display(employeeDF)

# COMMAND ----------

display(employeeDF)

display(employeeDF.filter(employeeDF.salary == 15000)) # ==, >, <, <=, >=, !=

# COMMAND ----------

display(employeeDF)
# & , |
# display(employeeDF.filter((employeeDF.gender == 'F') & (employeeDF.doj == 2004)))

display(employeeDF.filter((employeeDF.gender == 'F') | (employeeDF.doj == 2004)))

# COMMAND ----------

display(employeeDF)

# startswith, endswith, contains
# display(employeeDF.filter(employeeDF.name.startswith('V')))
# display(employeeDF.filter(employeeDF.name.endswith('r')))
display(employeeDF.filter(employeeDF.name.contains('Si')))

# COMMAND ----------

display(employeeDF)

# isNull, isNotNull
# display(employeeDF.filter(employeeDF.gender.isNull()))
display(employeeDF.filter(employeeDF.gender.isNotNull()))

# COMMAND ----------

display(employeeDF)

# isin
display(employeeDF.filter(employeeDF.employee_dept_id.isin(100,200)))

# COMMAND ----------

display(employeeDF.filter(employeeDF.name.like('%inay%')))
