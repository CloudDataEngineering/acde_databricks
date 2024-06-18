# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### DataBricks Secret Scope Creation
# MAGIC 1. We need to create the 'Azure Key Vault' before creating the Databricks Secret Scope. 
# MAGIC 2. Make sure you select the `Permission model` as `Vault access policy` while crete `Azure Key Vault` other wise you will get the permission error.
# MAGIC 3. Once the Azure Key Vault has been created then we need to create the Databricks Secret Scope.
# MAGIC 4. To create a Databricks Secret Scope goto the Databricks workspace click on the left corner 'Microsoft Azure' to get the proper URL.
# MAGIC 5. Once the URL look like this 'https://adb-303264221704821.1.azuredatabricks.net/?o=303264221704821#' then add the following line to open the Databricks Secret Scope '/secrets/createScope'.
# MAGIC 6. finally the URL look like this 'https://adb-303264221704821.1.azuredatabricks.net/?o=303264221704821#secrets/createScope'
# MAGIC 7. Once the 'Create Secret Scope' window opens then fill the following.
# MAGIC
# MAGIC - `Scope Name` : 'Provide the Scope Name'
# MAGIC
# MAGIC ####Azure Key Vault
# MAGIC
# MAGIC - `DNS Name` : Open the Azure Key Vault Navigate to '`Properties`' under '`Settings`' copy the '`Vault URL`' and past it into DNS Name.
# MAGIC - `Resource ID` : Open the Azure Key Vault Navigate to '`Properties`' under '`Settings`' copy the '`Resource ID`' and past it into Resource ID.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify the list of Scret-Scope has been created using the follwing command.
# MAGIC - dbutils.secrets.listScopes()

# COMMAND ----------

# DBTITLE 1,List Scopes
# dbutils.secrets.listScopes()
display(dbutils.secrets.listScopes())

# COMMAND ----------

# MAGIC %md
# MAGIC #### To view the list of Secrits(Azure Key vault secret) are available in specific Secrit-Scope use the following command.
# MAGIC - 

# COMMAND ----------

dbutils.secrets.list(scope='lti-scope')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create new `Azure Secrets` goto `Secrets` under the `Objects` Click on `Generate/Import`

# COMMAND ----------

display(dbutils.secrets.list(scope='lti-scope'))

# COMMAND ----------

dbutils.secrets.get(scope= 'lti-scope', key = 'acdeadls')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a Variable to assain the Secreates to useit into creat access between Azure and Databricks as code follows.
# MAGIC
# MAGIC - variable_name = dbutils.secrets.get(scope= 'scope name', key = 'key name')

# COMMAND ----------

adls_account_key = dbutils.secrets.get(scope= 'lti-scope', key = 'acdeadls')

# COMMAND ----------

display(adls_account_key)
