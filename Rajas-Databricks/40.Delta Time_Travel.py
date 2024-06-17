# Databricks notebook source
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
# MAGIC select * from scd2Demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel

# COMMAND ----------

# DBTITLE 1,Table History
# MAGIC %sql
# MAGIC describe history scd2demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark Approaches

# COMMAND ----------

# DBTITLE 1,Method 1: Pyspark - TimeStamp + Table
df = spark.read \
        .format('delta') \
        .option('timestampAsOf', '2024-04-21T06:35:28Z')\
        .table('scd2Demo')

display(df)

# COMMAND ----------

# DBTITLE 1,Method 2: Pyspark - TimeStamp + Path
df = spark.read \
        .format('delta') \
        .option('timestampAsOf', '2024-04-21T06:35:28Z')\
        .load('/FileStore/tables/delta/scd2Demo')

display(df)

# COMMAND ----------

# DBTITLE 1,Method 3: Pyspark - Version + Path
df = spark.read \
        .format('delta') \
        .option('versionAsof', '2')\
        .load('/FileStore/tables/delta/scd2Demo')

display(df)

# COMMAND ----------

# DBTITLE 1,Method 4: Pyspark - Version + Table
df = spark.read \
        .format('delta') \
        .option('versionAsof', '2')\
        .table('scd2Demo')

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Approaches

# COMMAND ----------

# DBTITLE 1,Method 5: SQL - Version + Table 
# MAGIC %sql
# MAGIC SELECT * FROM scd2demo VERSION AS OF 2

# COMMAND ----------

# DBTITLE 1,Method 6: SQL - Version + Path
# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`/FileStore/tables/delta/scd2Demo` VERSION AS OF 2

# COMMAND ----------

# DBTITLE 1,Method 7: SQL - Timestamp + Table 
# MAGIC %sql
# MAGIC SELECT * FROM scd2demo TIMESTAMP AS OF '2024-04-21T06:35:28Z'

# COMMAND ----------

# DBTITLE 1,Method 8: SQL - Timestamp + Path
# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`/FileStore/tables/delta/scd2Demo` TIMESTAMP AS OF '2024-04-21T06:35:28Z'
