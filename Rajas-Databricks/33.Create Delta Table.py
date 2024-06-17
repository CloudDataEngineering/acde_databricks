# Databricks notebook source
# DBTITLE 1,Method 1: Pyspark
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

# COMMAND ----------

from delta.tables import *

DeltaTable.createIfNotExists(spark) \
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

from delta.tables import *

DeltaTable.createOrReplace(spark) \
    .tableName('default.employee_demo') \
    .addColumn('emp_id', 'INT') \
    .addColumn('emp_name', 'STRING') \
    .addColumn('gender', 'STRING') \
    .addColumn('salary', 'INT') \
    .addColumn('Dept', 'STRING') \
    .property('description', 'table created for employee') \
    .location('dbfs:/FileStore/tables/delta/02_emp_table') \
    .execute()

# COMMAND ----------

# DBTITLE 1,Method 2: SQL
# MAGIC %sql
# MAGIC create table employee_demo_sql(
# MAGIC   emp_id int,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo_sql;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists employee_demo_sql(
# MAGIC   emp_id int,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table default.employee_demo_sql(
# MAGIC   emp_id int,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) using delta
# MAGIC location 'dbfs:/user/hive/warehouse/employee_demo_sql'

# COMMAND ----------

# DBTITLE 1,Method 3: Using Dataframe
data_student = [('Vinay', 'Science', 88, 'P', 90),
                 ('Raj', ' Maths', 75,'P', None),
                 ('Siva', ' English', 90, 'P', 80),
                 ('Raja', ' Science',  45, 'F', 75),
                 ('Rama', ' Maths', None, 'F', None),
                 ('Rasul', None, 30, 'F', 50),
                 ('Kumar', ' Social', None, None, 70)
                 ]

schema = ['name', 'subject', 'mark', 'status', 'attendance']

df = spark.createDataFrame(data = data_student, schema = schema)
display(df)


# COMMAND ----------

# Create table in the metastore using Dataframe's Schema and write data to it
df.write.format('delta').saveAsTable('data_student')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_student;
