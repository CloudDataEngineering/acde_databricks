# Databricks notebook source
# DBTITLE 1,JDBC Connection Definition
jdbcHostname = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcHostname')
jdbcPort = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcPort')
jdbcDatabase = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcDatabase')
jdbcUsername = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcUsername')
jdbcPassword = dbutils.secrets.get(scope= 'akv_secret', key= 'jdbcPassword')
jdbcDriver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

jdbcUrl = f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}'

# COMMAND ----------

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
