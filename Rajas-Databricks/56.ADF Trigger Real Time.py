# Databricks notebook source
# dbutils.widgets.text('outputPath', '')
# dbutils.widgets.text('table', '')

# COMMAND ----------

tableName = dbutils.widgets.get('table')
outputLocation = dbutils.widgets.get('outputPath')
print('tableName is:',tableName,'and the outputLocation is :',outputLocation)

# COMMAND ----------

# DBTITLE 1,JDBC Connection Definition
jdbcHostname = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcHostname')
jdbcPort = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcPort')
jdbcDatabase = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcDatabase')
jdbcUsername = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcUsername')
jdbcPassword = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcPassword')
jdbcDriver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

jdbcUrl = f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}'

# COMMAND ----------

# DBTITLE 1,Read from Azure SQL Database
sales_address = spark.read.format('jdbc').option('url', jdbcUrl).option('dbtable','SalesLT.Address').load()
display(sales_address)

# COMMAND ----------

dbutils.fs.unmount('/mnt/demo')

# COMMAND ----------

# DBTITLE 1,Create Mount Point to Integrate with ADLS
container = dbutils.secrets.get(scope= 'akv_secret', key= 'container')
storageaccount = dbutils.secrets.get(scope= 'akv_secret', key= 'storageaccount')
accesskey = dbutils.secrets.get(scope= 'akv_secret', key= 'accesskey')

dbutils.fs.mount(
    source = f'wasbs://{container}@{storageaccount}.blob.core.windows.net/',
    mount_point = '/mnt/demo',
    extra_configs = 
    {f'fs.azure.account.key.{storageaccount}.blob.core.windows.net':f'{accesskey}'}
)

# COMMAND ----------

# DBTITLE 1,Write Dataframe into ADLS
from datetime import datetime

outPath = '/mnt/demo/address/sales_address_'+datetime.now().strftime('%Y%m%d%H%M%S')+'/'

sales_address.write.format('csv').option('header', True).option('sep', ',').save(outPath)

# COMMAND ----------

# DBTITLE 1,Calculate Number of records Processed
totalCount = sales_address.count()
display(totalCount)

# COMMAND ----------

# DBTITLE 1,Return Output Patameter to ADF
dbutils.notebook.exit(totalCount)

# COMMAND ----------

# DBTITLE 1,Create pipeline log table
# MAGIC %sql
# MAGIC create table [dbo].[Pipe_Log] (
# MAGIC Pipeline_Name varchar(50),
# MAGIC Input_Table_Name varchar(25),
# MAGIC Output_Location varchar(250),
# MAGIC Processed_Time timestamp,
# MAGIC Record_Count int
# MAGIC );
# MAGIC
# MAGIC select * from [dbo].[Pipe_Log];

# COMMAND ----------

# DBTITLE 1,AccessToken
# Access-Token
