# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service pricipal
# MAGIC 1. Register Azure AD Application/ Service pricipal
# MAGIC 1. Generate a secreat/password for the Application
# MAGIC 1. Set the spark config with App/Client Id, Direcory/ Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

client_id = 'client_id'
tenant_id = 'tenant_id'
client_secret = 'client_secret'

# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

# spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
# spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

#---------------------------------------------------------------------------------------------

spark.conf.set("fs.azure.account.auth.type.adlsacde.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsacde.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsacde.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsacde.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsacde.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# dbutils.fs.ls('abfss://demo@adlsacde.dfs.core.windows.net')
display(dbutils.fs.ls('abfss://raw@adlsacde.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@adlsacde.dfs.core.windows.net/circuits.csv'))
