-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create Catalogs and Schemas required for the project
-- MAGIC 1. Catalog - formula1_dev (Without managed location)
-- MAGIC 2. Schemas - bronze, silver and gold (With managed location)

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1_dev;

-- COMMAND ----------

-- select current_catalog()
use catalog formula1_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_dev.bronze
MANAGED LOCATION 'abfss://bronze@databricksadlsext.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_dev.silver
MANAGED LOCATION 'abfss://silver@databricksadlsext.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_dev.gold
MANAGED LOCATION 'abfss://gold@databricksadlsext.dfs.core.windows.net/'

-- COMMAND ----------

show schemas
