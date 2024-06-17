# Databricks notebook source
# MAGIC %md
# MAGIC ##Method of ADLS Integration with Databricks
# MAGIC #### - Using a service principal
# MAGIC #### - Using Azure Active Directory credentials known as credential passthrough
# MAGIC #### - Using ADLS access key directly
# MAGIC #### - Creating Mount Point using ADLS Access Key

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 1 : Using ADLS access key directly

# COMMAND ----------

#Syntax
#-------
# spark.conf.set(
#     'fs.azure.account.key.<>.dfs.core.windows.net',
#     '<access Key>'
# )

# spark.conf.set(
#     'fs.azure.account.key.adlsvinay2907.dfs.core.windows.net',
#     'Account_key'
# )

# COMMAND ----------

#Syntax
# dbutils.fs.ls('abfss://<Container name>@<Storage account name>.dfs.core.windows.net/')

dbutils.fs.ls('abfss://demo@adlsvinay2907.dfs.core.windows.net/')

# COMMAND ----------

# set the data lake file location
file_location = 'abfss://trainingdb@adlsvinay2907.dfs.core.windows.net/Input_File_Name/1960_1965'

# read in the data to dataframe df
df = spark.read.format('csv').option('inferSchema','true').option('header', 'true').option('delimiter', ',').load(file_location)

# display the dataframe
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 2 : Creating Mount Point using ADLS Access Key

# COMMAND ----------

# Syntax
# --------
# dbutils.fs.mount(
#     source = 'wasbs://<Container name>@<Storage account name>.blob.core.windows.net',
#     mount_point = '/mnt/adls_test',
#     extra_configs = {'fs.azure.account.key.<>storageaccount.blob.core.windows.net':'<account key>'}
# )


# dbutils.fs.mount(
#     source = 'wasbs://trainingdb@adlsvinay2907.blob.core.windows.net/',
#     mount_point = '/mnt/Input_File_Name',
#     extra_configs = {'fs.azure.account.key.adlsvinay2907.blob.core.windows.net':'Account_key'}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/Input_File_Name')

# COMMAND ----------

# will list down all the mount point
dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount('/mnt/Input_File_Name')
