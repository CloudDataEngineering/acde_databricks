# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)

# COMMAND ----------

# dbutils.secrets.listScopes()
# dbutils.secrets.list('lti-scope')
client_id = dbutils.secrets.get(scope = "lti-scope", key = "client-id")
tenant_id = dbutils.secrets.get(scope = 'lti-scope', key = 'tenant-id')
client_secret = dbutils.secrets.get(scope = 'lti-scope', key = 'client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@acdeadls.dfs.core.windows.net/",
  mount_point = "/mnt/acdeadls/demo",
  extra_configs = configs)

# COMMAND ----------

# dbutils.fs.ls('abfss://demo@acdeadls.dfs.core.windows.net')
# display(dbutils.fs.ls('abfss://raw@acdeadls.dfs.core.windows.net'))
display(dbutils.fs.ls('/mnt/acdeadls/'))

# COMMAND ----------

# DBTITLE 1,spark read csv with header
display(spark.read.csv('/mnt/acdeadls/demo/circuits.csv',header = True))

# COMMAND ----------

# DBTITLE 1,dbutils.fs.mounts display
# dbutils.fs.mounts()
display(dbutils.fs.mounts())

# COMMAND ----------

# DBTITLE 1,Unmount Azure Data Lake Storage
# dbutils.fs.unmount('/mnt/acdeadls/demo')
