# Databricks notebook source
dbutils.fs.mount(
    source = 'wasbs://trainingdb@adlsvinay2907.blob.core.windows.net/',
    mount_point = '/mnt/adls',
    extra_configs = {'fs.azure.account.key.adlsvinay2907.blob.core.windows.net':'Account_key'}
)

# COMMAND ----------

root_path = "/mnt/adls/Input_File_Name/"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/adls/Input_File_Name/

# COMMAND ----------

from pyspark.sql import functions as F
df=spark.read.option('sep', ',').option('header', 'true').csv(root_path + '/*/*')

display(df)

# COMMAND ----------

dfNull = df.filter('Sex is null')
dfNull.display()

# COMMAND ----------

dfInputFileName = df.withColumn('input_file_name', F.input_file_name())
dfInputFileName.display()

# COMMAND ----------

dfInputFileName = df.withColumn('input_file_name', F.input_file_name()).filter('Sex is Null')
dfInputFileName.display()

# COMMAND ----------

df1 = dfInputFileName.withColumn('date_ingested', F.current_timestamp())
display(df1)
