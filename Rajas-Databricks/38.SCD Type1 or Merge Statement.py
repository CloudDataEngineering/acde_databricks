# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([StructField('emp_id', IntegerType(), True), 
                             StructField('name', StringType(), True), 
                             StructField('city', StringType(), True), 
                             StructField('country', StringType(), True), 
                             StructField('contact_number', IntegerType(), True)
                             ])

data = [(1000, 'Vinay Kumar M', 'Hyderabad', 'India',2147483646)]

df = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dim_employee(
# MAGIC   emp_id int,
# MAGIC   name STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   contact_number int
# MAGIC ) using delta
# MAGIC location 'dbfs:/FileStore/tables/delta/delta_merge'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# DBTITLE 1,Method 1: Spark SQL
df.createOrReplaceTempView('source_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# DBTITLE 1,Merge into
# MAGIC %sql
# MAGIC MERGE into dim_employee as target
# MAGIC using source_view as source
# MAGIC  ON target.emp_id= source.emp_id
# MAGIC  when matched
# MAGIC  then update set
# MAGIC  target.name = source.name,
# MAGIC  target.city = source.city,
# MAGIC  target.country = source.country,
# MAGIC  target.contact_number = source.contact_number
# MAGIC  when not matched then
# MAGIC  insert (emp_id, name, city, country, contact_number) values (emp_id, name, city, country, contact_number)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

data = [(1000, 'Vinay Kumar M', 'Bangalore', 'India',2147483646),
        (2000, 'Roopa K', 'Chennai', 'India',988501455)]

df = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView('source_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into dim_employee as target
# MAGIC using source_view as source
# MAGIC  ON target.emp_id= source.emp_id
# MAGIC  when matched
# MAGIC  then update set
# MAGIC  target.name = source.name,
# MAGIC  target.city = source.city,
# MAGIC  target.country = source.country,
# MAGIC  target.contact_number = source.contact_number
# MAGIC  when not matched then
# MAGIC  insert (emp_id, name, city, country, contact_number) values (emp_id, name, city, country, contact_number)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# DBTITLE 1,Method 2: Pyspark
data = [(1000, 'Vinay Kumar M', 'Bangalore', 'India',2147483646),
        (2000, 'Roopa K', 'Chennai', 'India',988501455)]

df = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

from delta.tables import *
delta_df = DeltaTable.forPath(spark, 'dbfs:/FileStore/tables/delta/delta_merge')

# COMMAND ----------

delta_df.alias('target').merge(
    source= df.alias('source'),
    condition = 'target.emp_id = source.emp_id'
).whenMatchedUpdate(set = {
    'name': 'source.name',
    'city': 'source.city',
    'country': 'source.country',
    'contact_number': 'source.contact_number'
    }
).whenNotMatchedInsert(values = {
     'emp_id': 'source.emp_id',
     'name': 'source.name',
     'city': 'source.city',
     'country': 'source.country',
     'contact_number': 'source.contact_number'
}
                       ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee;

# COMMAND ----------

# DBTITLE 1,Audit Log
# MAGIC %sql
# MAGIC CREATE TABLE audit_log(operation STRING,
# MAGIC                        updated_time TIMESTAMP,
# MAGIC                        user_name STRING,
# MAGIC                        notebook_name STRING,
# MAGIC                        numTargetRowsUpdatd INT,
# MAGIC                        numTargetRowsInserted INT,
# MAGIC                        numTargetRowsDeleted INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit_log

# COMMAND ----------

# DBTITLE 1,Create Dataframe with Last Operation in Delta Table
from delta.tables import *
delta_df = DeltaTable.forPath(spark, 'dbfs:/FileStore/tables/delta/delta_merge')

lastOperationDF = delta_df.history(1) # get the last operation
display(lastOperationDF)

# COMMAND ----------

# display(delta_df.history())
# display(delta_df.history(1))
display(delta_df.history(2))

# COMMAND ----------

# DBTITLE 1,Explode Operation Metrics Column
explode_df = lastOperationDF.select(lastOperationDF.operation, explode(lastOperationDF.operationMetrics))

explode_df_select = explode_df.select(explode_df.operation,explode_df.key,explode_df.value.cast('int'))

display(explode_df_select)

# COMMAND ----------

# DBTITLE 1,Pivot Operation to convert Rows to Columns
pivot_DF = explode_df_select.groupBy('operation').pivot('key').sum('value')
display(pivot_DF)

# COMMAND ----------

# DBTITLE 1,Select Only columns Needed for our Audit Log Table
pivot_DF_select = pivot_DF.select(pivot_DF.operation,pivot_DF.numTargetRowsUpdated,pivot_DF.numTargetRowsInserted,pivot_DF.numTargetRowsDeleted)

display(pivot_DF_select)

# COMMAND ----------

# DBTITLE 1,Add Notebook Paramenters such as User name, Notebook path etc,
auditDF = pivot_DF_select.withColumn('user_name',lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())).withColumn('notebook_name',lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())).withColumn('update_time', lit(current_timestamp()))

display(auditDF)

# COMMAND ----------

# DBTITLE 1,Rearranging Columns in Dataframe to Match it wiht Audit log Table
auditDF_select = auditDF.select(auditDF.operation,auditDF.update_time,auditDF.user_name,auditDF.notebook_name,auditDF.numTargetRowsUpdated,auditDF.numTargetRowsInserted,auditDF.numTargetRowsDeleted)

display(auditDF_select)

# COMMAND ----------

# DBTITLE 1,Create Temp View on Dataframe
auditDF_select.createOrReplaceTempView('audit')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit

# COMMAND ----------

# DBTITLE 1,Insert Audit Data into Audit Log Table
# MAGIC %sql
# MAGIC insert into audit_log
# MAGIC select * from audit

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit_log;
