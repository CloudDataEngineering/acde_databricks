# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the projects

# COMMAND ----------

# dbutils.fs.ls("/mnt/acdeadls/")
# dbutils.fs.mkdirs("/mnt/acdeadls/raw")
# dbutils.fs.mkdirs("/mnt/acdeadls/processed")
# dbutils.fs.mkdirs("/mnt/acdeadls/presentation")

# COMMAND ----------

# DBTITLE 1,Mount Azure Data Lake Storage
def mount_adls(storage_account_name, container_name):
    # Get Secrets from Key Vault
    client_id = dbutils.secrets.get(scope = "lti-scope", key = "client-id")
    tenant_id = dbutils.secrets.get(scope = 'lti-scope', key = 'tenant-id')
    client_secret = dbutils.secrets.get(scope = 'lti-scope', key = 'client-secret')

    # Set Spark Configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the storage account container if already mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount raw Container

# COMMAND ----------

mount_adls(storage_account_name = "acdeadls", container_name = "raw")

# COMMAND ----------

mount_adls(storage_account_name = "acdeadls", container_name = "processed")

# COMMAND ----------

mount_adls(storage_account_name = "acdeadls", container_name = "presentation")

# COMMAND ----------

mount_adls(storage_account_name = "acdeadls", container_name = "demo")

# COMMAND ----------

display(dbutils.fs.mounts())
