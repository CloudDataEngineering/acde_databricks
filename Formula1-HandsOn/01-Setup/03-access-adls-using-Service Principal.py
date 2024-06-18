# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service pricipal
# MAGIC 1. Register Azure AD Application/ Service pricipal
# MAGIC 1. Generate a secreat/password for the Application
# MAGIC 1. Set the spark config with App/Client Id, Direcory/ Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Search `Microsoft Entra ID` -> `App registrations` -> 
# MAGIC
# MAGIC - client_id = copy
# MAGIC - tenant_id = copy
# MAGIC - client_secret = Certificates & secrets -> create new secrets -> we need to copy the secret `Value`

# COMMAND ----------

# dbutils.secrets.listScopes()
dbutils.secrets.list(scope="lti-scope")

# COMMAND ----------

client_id = dbutils.secrets.get(scope="lti-scope", key="client-id")
tenant_id = dbutils.secrets.get(scope="lti-scope", key="tenant-id")
client_secret = dbutils.secrets.get(scope="lti-scope", key="client-secret")

# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

# spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
# spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

#---------------------------------------------------------------------------------------------

spark.conf.set("fs.azure.account.auth.type.acdeadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.acdeadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.acdeadls.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.acdeadls.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.acdeadls.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls('abfss://demo@acdeadls.dfs.core.windows.net')
# display(dbutils.fs.ls('abfss://raw@acdeadls.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@acdeadls.dfs.core.windows.net/circuits.csv'))
