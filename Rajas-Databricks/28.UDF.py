# Databricks notebook source
# DBTITLE 1,Create Sample Dataframe
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

# DBTITLE 1,Define UDF to Rename Columns
import pyspark.sql.functions as f

def rename_columns(rename_df):

    for column in rename_df.columns:
        # new_column = 'Col_'+column
        new_column = 'UDF_'+column
        rename_df = rename_df.withColumnRenamed(column, new_column)
    
    return rename_df


# COMMAND ----------

# DBTITLE 1,Execute UDF
renamed_df = rename_columns(employeeDF)
display(renamed_df)

# COMMAND ----------

# DBTITLE 1,UDF to convert name into upper case
from pyspark.sql.functions import upper, col

def upperCase_col(df):
    employeeDF_upper = df.withColumn('name_upper', upper(df.name))

    return employeeDF_upper

# COMMAND ----------

up_case_df = upperCase_col(employeeDF)
display(up_case_df)
