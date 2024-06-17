# Databricks notebook source
# MAGIC %md
# MAGIC #Creat Dataframe

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
# MAGIC # Add new column using constant literal

# COMMAND ----------

from pyspark.sql.functions import lit

empDF_AddCloumn = empDF.withColumn('Location',lit('Hyderabad'))

display(empDF_AddCloumn)

# COMMAND ----------

# MAGIC %md
# MAGIC #Add new column by calculation

# COMMAND ----------

from pyspark.sql.functions import lit

empDF_AddCloumn = empDF.withColumn('Bonus',empDF.salary * 0.1)

display(empDF_AddCloumn)

# COMMAND ----------

from pyspark.sql.functions import concat

empDF_AddCloumn = empDF_AddCloumn.withColumn('Name',concat(lit('Mr. '),'first_name',lit(' '),'last_name'))

display(empDF_AddCloumn)

# COMMAND ----------

# MAGIC %md
# MAGIC #Rename a column

# COMMAND ----------

empDF_AddCloumn = empDF_AddCloumn.withColumnRenamed('Name','Full_Name').withColumnRenamed('doj','Data_of_Joining')
display(empDF_AddCloumn)

# COMMAND ----------

# MAGIC %md
# MAGIC #Drop a column

# COMMAND ----------

empDF_dropcolumn = empDF_AddCloumn.drop('Full_Name').drop('Bonus')
display(empDF_dropcolumn)
