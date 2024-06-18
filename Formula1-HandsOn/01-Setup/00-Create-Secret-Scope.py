# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### DataBricks Secret Scope Creation
# MAGIC 1. We need to create the 'Azure Key Vault' before creating the Databricks Secret Scope. once the Azure Key Vault has been created then we need to create the Databricks Secret Scope.
# MAGIC 2. To create a Databricks Secret Scope goto the Databricks workspace click on the left corner 'Microsoft Azure' to get the proper URL.
# MAGIC 3. Once the URL look like this 'https://adb-303264221704821.1.azuredatabricks.net/?o=303264221704821#' then add the following line to open the Databricks Secret Scope '/secrets/createScope'.
# MAGIC 4. finally the URL look like this 'https://adb-303264221704821.1.azuredatabricks.net/?o=303264221704821#secrets/createScope'
# MAGIC 5. Once the 'Create Secret Scope' window opens then fill the following.
# MAGIC
# MAGIC - `Scope Name` : 'Provide the Scope Name'
# MAGIC
# MAGIC ####Azure Key Vault
# MAGIC
# MAGIC - `DNS Name` : Open the Azure Key Vault Navigate to '`Properties`' under '`Settings`' copy the '`Vault URL`' and past it into DNS Name.
# MAGIC - `Resource ID` : Open the Azure Key Vault Navigate to '`Properties`' under '`Settings`' copy the '`Resource ID`' and past it into Resource ID.
