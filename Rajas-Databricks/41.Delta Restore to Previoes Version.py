# Databricks notebook source
# DBTITLE 1,Create table using SQL
# %sql
# CREATE OR REPLACE TABLE scd2Demo(
#                                   pk1 INT,
#                                   pk2 STRING,
#                                   dim1 INT,
#                                   dim2 INT,
#                                   dim3 INT,
#                                   dim4 INT,
#                                   active_status STRING,
#                                   start_date TIMESTAMP,
#                                   end_date TIMESTAMP)
#                             USING DELTA
#                             LOCATION '/FileStore/tables/delta/scd2Demo'

# COMMAND ----------

# DBTITLE 1,Create table using Pyspark
from delta.table import *

DeltaTable.create(spark) \
    .tableName('scd2Demo') \
    .addColumn('pk1', 'INT') \
    .addColumn('pk1', 'STRING') \
    .addColumn('dim1', 'INT') \
    .addColumn('dim2', 'INT') \
    .addColumn('dim3', 'INT') \
    .addColumn('dim4', 'INT') \
    .addColumn('active_status', 'STRING') \
    .addColumn('start_date', 'TIMESTAMP') \
    .addColumn('end_date', 'TIMESTAMP') \
    .location('/FileStore/tables/delta/scd2Demo') \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2Demo;

# COMMAND ----------

# DBTITLE 1,Create Delta Table Instance
from delta import *
targetTable = DeltaTable.forPath(spark, '/FileStore/tables/delta/scd2Demo')

# COMMAND ----------

# DBTITLE 1,List down Version History
display(targetTable.history())

# COMMAND ----------

# DBTITLE 1,Restore Using Version Number
targetTable.restoreToVersion(6) # restore table to oldest version

# COMMAND ----------

# DBTITLE 1,Restore Using Timestamp
targetTable.restoreToTimestamp('2024-04-21T07:53:16Z') # restore table to oldest version
