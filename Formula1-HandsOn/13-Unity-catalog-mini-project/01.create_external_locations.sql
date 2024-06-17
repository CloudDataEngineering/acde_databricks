-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create the external locations required for this project
-- MAGIC 1. Bronze
-- MAGIC 2. Silver
-- MAGIC 3. Gold

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_ext_bronze
URL 'abfss://bronze@databricksadlsext.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL databricks_adls_ext)

-- COMMAND ----------

DESC EXTERNAL LOCATION databricks_ext_bronze

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'abfss://bronze@databricksadlsext.dfs.core.windows.net/'

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_ext_silver
URL 'abfss://silver@databricksadlsext.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL databricks_adls_ext)

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_ext_gold
URL 'abfss://gold@databricksadlsext.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL databricks_adls_ext)

-- COMMAND ----------


