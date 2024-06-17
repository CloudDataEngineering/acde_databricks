# Databricks notebook source
# DBTITLE 1,Delta Table Creation
from delta.tables import *

DeltaTable.create(spark) \
    .tableName('employee_demo') \
    .addColumn('emp_id', 'INT') \
    .addColumn('emp_name', 'STRING') \
    .addColumn('gender', 'STRING') \
    .addColumn('salary', 'INT') \
    .addColumn('Dept', 'STRING') \
    .property('description', 'table created for employee') \
    .location('dbfs:/FileStore/tables/delta/02_emp_table') \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;
# MAGIC -- delete from employee_demo;

# COMMAND ----------

# DBTITLE 1,SQL Style Insert
# MAGIC %sql
# MAGIC insert into employee_demo values(100, 'Vinay Kumar M', 'M', 180000,'IT');
# MAGIC insert into employee_demo values(200, 'Roopa M', 'F', 60000,'HR');
# MAGIC insert into employee_demo values(300, 'Siva Kumar M', 'M', 80000,'Sales');

# COMMAND ----------

# DBTITLE 1,Dataframe Insert
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
emp_data = [(200, 'Roopa M', 'F', 60000,'HR')]

employee_schema = StructType([StructField('emp_id', IntegerType(), True), 
                             StructField('emp_name', StringType(), True), 
                             StructField('gender', StringType(), True), 
                             StructField('salary', IntegerType(), True), 
                             StructField('dept', StringType(), True)
                             ])
df = spark.createDataFrame(data = emp_data, schema = employee_schema)
display(df)

# COMMAND ----------

df.write.format('delta').mode('append').saveAsTable('employee_demo')

# COMMAND ----------

display(spark.sql('select * from employee_demo'))

# COMMAND ----------

# DBTITLE 1,Dataframe insert into Method
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
emp_data = [(300, 'Siva Kumar M', 'M', 80000,'Sales')]

employee_schema = StructType([StructField('emp_id', IntegerType(), True), 
                             StructField('emp_name', StringType(), True), 
                             StructField('gender', StringType(), True), 
                             StructField('salary', IntegerType(), True), 
                             StructField('dept', StringType(), True)
                             ])
df1 = spark.createDataFrame(data = emp_data, schema = employee_schema)
display(df1)

# COMMAND ----------

df1.write.insertInto('employee_demo', overwrite=False)

# COMMAND ----------

display(spark.sql('select * from employee_demo'))

# COMMAND ----------

# DBTITLE 1,Insert using Temp view
df1.createOrReplaceTempView('delta_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo
# MAGIC select * from delta_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------

# DBTITLE 1,Spark SQL Insert
spark.sql('insert into employee_demo select * from delta_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;
