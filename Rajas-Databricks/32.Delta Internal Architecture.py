# Databricks notebook source
# DBTITLE 1,Create  Delta Table
from delta.tables import *

DeltaTable.create(spark) \
    .tableName('delta_internal_demo') \
    .addColumn('emp_id', 'INT') \
    .addColumn('emp_name', 'STRING') \
    .addColumn('gender', 'STRING') \
    .addColumn('salary', 'INT') \
    .addColumn('Dept', 'STRING') \
    .property('description', 'table created for demo purpose') \
    .location('dbfs:/FileStore/tables/delta/01_arch_demo') \
    .execute()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/01_arch_demo/_delta_log

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/FileStore/tables/delta/01_arch_demo/_delta_log/00000000000000000002.json

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update delta_internal_demo set emp_id = 300 where salary = 80000;
# MAGIC select * from delta_internal_demo order by emp_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_internal_demo values(100, 'Vinay Kumar M', 'M', 180000,'IT');

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_internal_demo values(200, 'Roopa M', 'F', 60000,'HR');
# MAGIC insert into delta_internal_demo values(200, 'Siva Kumar M', 'M', 80000,'Sales');

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_internal_demo values(100, 'Vinay Kumar M', 'M', 180000,'IT');
# MAGIC insert into delta_internal_demo values(200, 'Roopa M', 'F', 60000,'HR');
# MAGIC insert into delta_internal_demo values(300, 'Siva Kumar M', 'M', 80000,'Sales');
# MAGIC
# MAGIC insert into delta_internal_demo values(100, 'Vinay Kumar M', 'M', 180000,'IT');
# MAGIC insert into delta_internal_demo values(200, 'Roopa M', 'F', 60000,'HR');
# MAGIC insert into delta_internal_demo values(3000, 'Siva Kumar M', 'M', 80000,'Sales');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete from delta_internal_demo where emp_id = 100;
# MAGIC
# MAGIC select * from delta_internal_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table delta_internal_demo;
