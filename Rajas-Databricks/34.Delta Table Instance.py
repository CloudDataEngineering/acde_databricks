# Databricks notebook source
# MAGIC %md
# MAGIC #Delta Table Instance
# MAGIC
# MAGIC * Detla Table instance is replace of delta table
# MAGIC * It is mainly used to perform DML operations on delta table using Pyspark language.
# MAGIC * It can be created using 2 ways
# MAGIC
# MAGIC ## Approach 1: Using for Path
# MAGIC ## Approach 2: Using for Table Name
# MAGIC
# MAGIC ## Syntax
# MAGIC
# MAGIC * deltaTable = DeltaTable.forPath(spark, '/path/to/table')
# MAGIC * deltaTable = DeltaTable.forPath(spark, 'table_name')

# COMMAND ----------

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
# MAGIC insert into employee_demo values(100, 'Vinay Kumar M', 'M', 180000,'IT');
# MAGIC insert into employee_demo values(200, 'Roopa M', 'F', 60000,'HR');
# MAGIC insert into employee_demo values(300, 'Siva Kumar M', 'M', 80000,'Sales');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo order by emp_id;

# COMMAND ----------

deltaInstance1 = DeltaTable.forPath(spark, 'dbfs:/FileStore/tables/delta/02_emp_table')

# COMMAND ----------

display(deltaInstance1.toDF().orderBy('emp_id'))

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from employee_demo where emp_id = 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo order by emp_id;

# COMMAND ----------

deltaInstance1.delete('emp_id=200')

# COMMAND ----------

display(deltaInstance1.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secound Approach

# COMMAND ----------

deltaInstance2 = DeltaTable.forName(spark, 'employee_demo')

# COMMAND ----------

display(deltaInstance2.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employee_demo

# COMMAND ----------

display(deltaInstance2.history())
