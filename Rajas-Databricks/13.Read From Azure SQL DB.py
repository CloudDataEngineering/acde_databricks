# Databricks notebook source
# MAGIC %md
# MAGIC ##Define JDBC Connection Parameters

# COMMAND ----------

jdbcHostname = 'asdbserver.database.windows.net'
jdbcPort = 1433 
jdbcDatabase = 'asdbdev'
jdbcUsername = 'sadmin'
jdbcPassword = 'Welcome@123'
jdbcDriver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

jdbcUrl = f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}'

# COMMAND ----------

df1 = spark.read.format('jdbc').option('url',jdbcUrl).option('dbtable','SalesLT.Customer').load()
display(df1)

# COMMAND ----------

connetionString = 'jdbc:sqlserver://asdbserver.database.windows.net:1433;database=asdbdev;user=sadmin@asdbserver;password={Welcome@123};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'

# COMMAND ----------

df = spark.read.jdbc(connetionString,'SalesLT.Address')
display(df)

# COMMAND ----------

df1 = spark.sql('select * from SalesLT.Address')
