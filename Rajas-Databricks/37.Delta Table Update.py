# Databricks notebook source
# DBTITLE 1,Delta Table Creation
# MAGIC %sql
# MAGIC create or replace table default.employee_demo_sql(
# MAGIC   emp_id int,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) using delta
# MAGIC location 'dbfs:/FileStore/tables/delta/emp_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo_sql;
# MAGIC -- delete from employee_demo_sql;

# COMMAND ----------

# DBTITLE 1,Populate sample data
# MAGIC %sql
# MAGIC insert into employee_demo_sql values(100, 'Vinay Kumar M', 'M', 180000,'IT');
# MAGIC insert into employee_demo_sql values(200, 'Roopa M', 'F', 60000,'HR');
# MAGIC insert into employee_demo_sql values(300, 'Siva Kumar M', 'M', 80000,'Sales');
# MAGIC insert into employee_demo_sql values(400, 'Hamsini M', 'M', 180000,'IT');
# MAGIC insert into employee_demo_sql values(500, 'MMM', 'F', 60000,'HR');
# MAGIC insert into employee_demo_sql values(600, 'Sujana M', 'M', 80000,'Sales');
# MAGIC insert into employee_demo_sql values(700, 'Subbu M', 'M', 80000,'Sales');

# COMMAND ----------

# DBTITLE 1,Method 1: SQL using delta location
# MAGIC %sql
# MAGIC UPDATE delta.'dbfs:/FileStore/tables/delta/emp_table' set salary = 1254087 where emp_id = 200

# COMMAND ----------

# DBTITLE 1,Using Spark SQL
spark.sql('delete from employee_demo where emp_id = 200')

# COMMAND ----------

# DBTITLE 1,Pyspark Delta table Instance
from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 'dbfs:/FileStore/tables/delta/emp_table')

# Declare the predicate by using a SQL-formatted string
deltaTable.update(
    condition = "emp_name = 'Vinay Kumar M'",
    set = {"salary": "250000"})

# COMMAND ----------

# DBTITLE 1,Multiple conditions using Sql predicate
deltaTable.delete("emp_id = 500 and gender = 'F'")

# COMMAND ----------

display(spark.sql('select * from employee_demo_sql'))

# COMMAND ----------

# DBTITLE 1,Pyspark Delta Table Instance - Spark SQL Predicate
# Declare the predicate by using Spark SQLfunction.
deltaTable.update(condition = col('emp_name') == 'Roopa M',
                  set= {'dept': lit('IT')}
                  )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo_sql
