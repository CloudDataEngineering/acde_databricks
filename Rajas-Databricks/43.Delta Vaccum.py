# Databricks notebook source
# DBTITLE 1,Create table using SQL
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE scd2Demo(
# MAGIC                                   pk1 INT,
# MAGIC                                   pk2 STRING,
# MAGIC                                   dim1 INT,
# MAGIC                                   dim2 INT,
# MAGIC                                   dim3 INT,
# MAGIC                                   dim4 INT,
# MAGIC                                   active_status STRING,
# MAGIC                                   start_date TIMESTAMP,
# MAGIC                                   end_date TIMESTAMP)
# MAGIC                             USING DELTA
# MAGIC                             LOCATION '/FileStore/tables/delta/scd2Demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo values (111, 'Unit1', 200, 500, 800, 400, 'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo values (222, 'Unit2', 900, Null, 700, 100, 'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo values (333, 'Unit3', 300, 900, 250, 650, 'Y',current_timestamp(),'9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo values (666, 'Unit1', 200, 500, 800, 400, 'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo values (777, 'Unit2', 900, Null, 700, 100, 'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo values (888, 'Unit3', 300, 900, 250, 650, 'Y',current_timestamp(),'9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from default.scd2Demo where pk1 = '777'

# COMMAND ----------

# MAGIC %sql
# MAGIC update scd2demo set dim1 = 100 where pk1 = 666

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2Demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history scd2demo

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize scd2demo

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/delta/scd2Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM scd2Demo DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM scd2Demo RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = False

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM scd2Demo RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM scd2Demo