# Databricks notebook source
# dbutils.fs.rm('dbfs:/FileStore/tables/delta/emp_demo', True)

# COMMAND ----------

# DBTITLE 1,Delta Table Creation
from delta.tables import *

DeltaTable.create(spark) \
    .tableName('employee_demo') \
    .addColumn('\emp_id', 'INT') \
    .addColumn('emp_name', 'STRING') \
    .addColumn('gender', 'STRING') \
    .addColumn('salary', 'INT') \
    .addColumn('Dept', 'STRING') \
    .property('description', 'table created for employee') \
    .location('dbfs:/FileStore/tables/delta/emp_demo') \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;
# MAGIC -- delete from employee_demo;

# COMMAND ----------

# DBTITLE 1,Populate sample data
# MAGIC %sql
# MAGIC insert into employee_demo values(100, 'Vinay Kumar M', 'M', 180000,'IT');
# MAGIC -- insert into employee_demo values(200, 'Roopa M', 'F', 60000,'HR');
# MAGIC -- insert into employee_demo values(300, 'Siva Kumar M', 'M', 80000,'Sales');
# MAGIC -- insert into employee_demo values(400, 'Hamsini M', 'M', 180000,'IT');
# MAGIC -- insert into employee_demo values(500, 'MMM', 'F', 60000,'HR');
# MAGIC -- insert into employee_demo values(600, 'Sujana M', 'M', 80000,'Sales');
# MAGIC -- insert into employee_demo values(700, 'Subbu M', 'M', 80000,'Sales');

# COMMAND ----------

# DBTITLE 1,Schema Evolution
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
emp_data = [(200, 'Roopa M', 'F', 60000,'HR','Test data')]

employee_schema = StructType([StructField('emp_id', IntegerType(), True), 
                             StructField('emp_name', StringType(), True), 
                             StructField('gender', StringType(), True), 
                             StructField('salary', IntegerType(), True), 
                             StructField('dept', StringType(), True),
                             StructField('additionalCol', StringType(), True)
                             ])
df = spark.createDataFrame(data = emp_data, schema = employee_schema)
display(df)

# COMMAND ----------

df.write.format('delta').mode('append').saveAsTable('employee_demo')

# COMMAND ----------

df.write.option('mergeSchema', 'true').format('delta').mode('append').saveAsTable('employee_demo')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
emp_data = [(300, 'Siva Kumar M', 'M', 80000,'Sales','Test data1')]

employee_schema = StructType([StructField('emp_id', IntegerType(), True), 
                             StructField('emp_name', StringType(), True), 
                             StructField('gender', StringType(), True), 
                             StructField('salary', IntegerType(), True), 
                             StructField('dept', StringType(), True),
                             StructField('additionalCol1', StringType(), True)
                             ])
df = spark.createDataFrame(data = emp_data, schema = employee_schema)
display(df)

# COMMAND ----------

df.write.option('mergeSchema', 'true').format('delta').mode('append').saveAsTable('employee_demo')
