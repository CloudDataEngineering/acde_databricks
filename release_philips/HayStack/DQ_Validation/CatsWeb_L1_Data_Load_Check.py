# Databricks notebook source
from delta.tables import *
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import date, timedelta


# COMMAND ----------

# MAGIC %md
# MAGIC #### Set and Get Parameters
# MAGIC

# COMMAND ----------

dbutils.widgets.text("LastLoadedDate", "", "LastLoadedDate")
dbutils.widgets.text("SourceName", "", "SourceName")
dbutils.widgets.text("ADF_Name", "", "ADF_Name")

LastLoadedDate = dbutils.widgets.get("LastLoadedDate")
SourceName = dbutils.widgets.get("SourceName")
ADF_Name = dbutils.widgets.get("ADF_Name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set ENV variable

# COMMAND ----------

if ADF_Name == 'az23d1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'dev_l2'
elif ADF_Name == 'az23q1-gf-quality-adf':
    Catalog_Name_L1 = 'qa_l1'
    Catalog_Name_L2 = 'qa_l2'
elif ADF_Name == 'az23t1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'dev_l2'
elif ADF_Name == 'az23p1-gf-quality-adf':
    Catalog_Name_L1 = 'prod_l1'
    Catalog_Name_L2 = 'prod_l2'

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS dev_l2.qnr.l1_audit
# LOCATION  'abfss://silver@az21d1datalakewe.dfs.core.windows.net//im/QDS/Audit_TableComparisionCount_ALL';

# COMMAND ----------

df_result_Status_count = spark.sql(f"""select count(1) from {Catalog_Name_L2}.qnr.l1_audit_dq_monitoring_catsweb_log where Run_date = '{LastLoadedDate}' and ExtractionType = 'L1 Data Load'""").collect()[0][0]

print(df_result_Status_count)
if df_result_Status_count > 0:
    dbutils.notebook.exit('L1 Data Loaded')
else:
    dbutils.notebook.exit('L1 Data not Loaded')

# COMMAND ----------


