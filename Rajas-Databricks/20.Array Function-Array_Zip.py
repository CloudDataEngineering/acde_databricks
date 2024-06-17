# Databricks notebook source
# DBTITLE 1,Create Sample Dataframe
array_data = [
    ('John', 4, 1),
    ('John', 6, 2),
    ('David', 7, 3),
    ('Mike', 3, 4),
    ('David', 5, 2),
    ('John', 7, 3),
    ('John', 9, 7),
    ('David', 1, 8),
    ('David', 4, 9),
    ('David', 7, 4),
    ('Mike', 8, 5),
    ('Mike', 5, 2),
    ('Mike', 3, 8),
    ('John', 2, 7),
    ('David', 1, 9)
]

array_schema = ['Name', 'Score_1', 'Score_2']

arrayDF = spark.createDataFrame(data = array_data, schema = array_schema)

display(arrayDF)

# COMMAND ----------

# DBTITLE 1,Convert Sample Dataframe into Array Dataframe
from pyspark.sql import functions as F

masterDF = arrayDF.groupby('Name').agg(F.collect_list('Score_1').alias('Array_Score_1'),F.collect_list('Score_2').alias('Array_Score_2'))

display(masterDF)
masterDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Apply arrays_zip function on ArrayDF
arr_zip_df = masterDF.withColumn('Zipped_value', F.arrays_zip('Array_Score_1','Array_Score_2'))

arr_zip_df.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Practical Use Case to flatten data using array_zip and explode

# COMMAND ----------

# DBTITLE 1,Create Sample Dataframe
empDF = [
    ('Sales_dept',[{'emp_name': 'John', 'salary': '1000','yrs_of_service': '10', 'Age': '33'},
                   {'emp_name': 'David', 'salary': '1500','yrs_of_service': '7', 'Age': '35'},
                   {'emp_name': 'Nancy', 'salary': '2000','yrs_of_service': '15', 'Age': '43'},
                   {'emp_name': 'Mike', 'salary': '1700','yrs_of_service': '12', 'Age': '50'},
                   {'emp_name': 'Rosy', 'salary': '1400','yrs_of_service': '8', 'Age': '61'}]),
    ('HR_dept',[{'emp_name': 'Binay', 'salary': '1600','yrs_of_service': '16', 'Age': '74'},
                   {'emp_name': 'Vinay', 'salary': '5000','yrs_of_service': '11', 'Age': '68'},
                   {'emp_name': 'Roopa', 'salary': '4000','yrs_of_service': '18', 'Age': '58'},
                   {'emp_name': 'Siva', 'salary': '3000','yrs_of_service': '6', 'Age': '52'},
                   {'emp_name': 'MMM', 'salary': '8000','yrs_of_service': '9', 'Age': '31'}])
]

df_brand = spark.createDataFrame(data = empDF, schema = ['Deparment', 'Employee'])
df_brand.printSchema()
display(df_brand)

# COMMAND ----------

# DBTITLE 1,Apply Array_zip
df_bandZip = df_brand.withColumn('Zip',F.arrays_zip(df_brand['Employee']))
display(df_bandZip)

# COMMAND ----------

# DBTITLE 1,Apply Explode
df_brand_exp = df_bandZip.withColumn('Explode',F.explode(df_bandZip.Zip))
display(df_brand_exp)

# COMMAND ----------

# DBTITLE 1,Flatten fields from exploded list
df_brand_output = df_brand_exp.withColumn('employee_emp_name',df_brand_exp['Explode.Employee.emp_name']) \
    .withColumn('employee_yrs_of_service',df_brand_exp['Explode.Employee.yrs_of_service']) \
        .withColumn('employee_salary',df_brand_exp['Explode.Employee.salary']) \
            .withColumn('employee_Age',df_brand_exp['Explode.Employee.Age']).drop('Explode').drop('Zip').drop('Employee')

display(df_brand_output)

# COMMAND ----------

# MAGIC %md
# MAGIC # Array Function - Array Intersect

# COMMAND ----------

# DBTITLE 1,Create Sample Dataframe
empDF = [
        ('John', [4, 6, 7, 9, 2], [1, 2, 3, 7, 7]),
        ('David', [7, 5, 1, 4, 7, 1], [3, 2, 8, 9, 4, 9]),
        ('Mike', [3, 9, 1, 6, 2], [1, 2, 3, 5, 8])
        ]

df = spark.createDataFrame(data = empDF, schema = ['Name', 'Array_1', 'Array_2'])
df.show()

# COMMAND ----------

# DBTITLE 1,Applay Array_intersect
from pyspark.sql import functions as F

df_intersect = df.withColumn('Intersect', F.array_intersect(df['Array_1'], df['Array_2']))

df_intersect.show()

# COMMAND ----------

# DBTITLE 1,Applay Array_except
from pyspark.sql import functions as F

df_except = df.withColumn('Intersect', F.array_except(df['Array_1'], df['Array_2']))

df_except.show()

# COMMAND ----------

# DBTITLE 1,Applay Array_Sort
from pyspark.sql import functions as F

df_sort = df.withColumn('Sorted', F.array_sort(df['Array_1']))

df_sort.show()
