# Databricks notebook source
# MAGIC %md
# MAGIC ## Pyspark Function SPLIT

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

# DBTITLE 1,First Method of Split
from pyspark.sql.functions import split

df1 = empDF.withColumn('First_Name', split(empDF['Name'],' ').getItem(0)).withColumn('Last_Name', split(empDF['Name'],' ').getItem(1))

display(df1)

# COMMAND ----------

# DBTITLE 1,Second Method of Split
import pyspark

split_col = pyspark.sql.functions.split(empDF['Name'], ' ')

df2 = empDF.withColumn('First_Name', split_col.getItem(0)) \
            .withColumn('Last_Name', split_col.getItem(1))

display(df2)

# COMMAND ----------

# DBTITLE 1,Third Method of Split
split_col = pyspark.sql.functions.split(empDF['doj'], '-')

# df3 = empDF.withColumn('Year', split_col.getItem(0)) \
#             .withColumn('Month', split_col.getItem(1)) \
#                 .withColumn('Date', split_col.getItem(2))

df3 = empDF.select('employee_id', 'Name', 'doj', 'employee_dept_id', 'gender', 'salary', split_col.getItem(0).alias('Joining_Year'),split_col.getItem(1).alias('Joining_Month'),split_col.getItem(2).alias('Joining_Date'))

display(df3)

# COMMAND ----------

# DBTITLE 1,Combine multiple split
df4 = empDF.withColumn('First_Name', split(empDF['Name'], ' ').getItem(0)) \
            .withColumn('Last_Name', split(empDF['Name'], ' ').getItem(1)) \
            .withColumn('Joining_Year', split(empDF['doj'], '-').getItem(0)) \
            .withColumn('Joining_Month', split(empDF['doj'], '-').getItem(1)) \
            .withColumn('Joining_Date', split(empDF['doj'], '-').getItem(2))

display(df4)

# COMMAND ----------

# DBTITLE 1,Split and Drop splitted columns
df5 = empDF.withColumn('First_Name', split(empDF['Name'], ' ').getItem(0)) \
            .withColumn('Last_Name', split(empDF['Name'], ' ').getItem(1)) \
            .withColumn('Joining_Year', split(empDF['doj'], '-').getItem(0)) \
            .withColumn('Joining_Month', split(empDF['doj'], '-').getItem(1)) \
            .withColumn('Joining_Date', split(empDF['doj'], '-').getItem(2)) \
            .drop(empDF['Name']) \
            .drop(empDF['doj'])

display(df5)
