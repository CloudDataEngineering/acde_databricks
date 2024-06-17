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
# MAGIC insert into employee_demo values(200, 'Roopa M', 'F', 60000,'HR');
# MAGIC insert into employee_demo values(300, 'Siva Kumar M', 'M', 80000,'Sales');
# MAGIC insert into employee_demo values(400, 'Hamsini M', 'M', 180000,'IT');
# MAGIC insert into employee_demo values(500, 'MMM', 'F', 60000,'HR');
# MAGIC insert into employee_demo values(600, 'Sujana M', 'M', 80000,'Sales');
# MAGIC insert into employee_demo values(700, 'Subbu M', 'M', 80000,'Sales');

# COMMAND ----------

# DBTITLE 1,Method 1: SQL using delta location
# MAGIC %sql
# MAGIC DELETE FROM delta.'/FileStore/tables/delta/emp_demo' where emp_id = 200

# COMMAND ----------

# DBTITLE 1,Using Spark SQL
spark.sql('delete from employee_demo where emp_id = 200')

# COMMAND ----------

# DBTITLE 1,Pyspark Delta table Instance
from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forName(spark, 'employee_demo')

# Declare the predicate by using a SQL-formatted string
deltaTable.delete('emp_id = 400')

# COMMAND ----------

# DBTITLE 1,Multiple conditions using Sql predicate
deltaTable.delete("emp_id = 500 and gender = 'F'")

# COMMAND ----------

display(spark.sql('select * from employee_demo'))

# COMMAND ----------

# DBTITLE 1,Pyspark Delta Table Instance - Spark SQL Predicate
# Declare the predicate by using Spark SQLfunction.
deltaTable.delete(col('emp_id') == 700)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo
