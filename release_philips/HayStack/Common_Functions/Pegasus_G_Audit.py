# Databricks notebook source
# DBTITLE 1,# import libraries
import json
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType,LongType
import pandas as pd
from pyspark.sql import functions as f
from datetime import datetime
from pyspark.sql import SparkSession


# COMMAND ----------

# MAGIC %run /Shared/release/HayStack/Common_Functions/Haystack_G_Merge

# COMMAND ----------

# DBTITLE 1,Generic Function to update data in Audit table

def Audit_G_logging (Catalog_Name_WB,Audit_Job_ID,Audit_Run_ID ,Audit_Start_date_Time ,Audit_End_Date_Time ,Audit_Source_Name ,Audit_Table_Name ,Audit_Status,Audit_Count,Audit_Max_ID,Audit_Min_ID,Audit_Error_Message,l1_last_run_date,g_last_run_date ):
    
    #Catalog = Audit_Table_Name.split('.')[0]
    Audit_log_table = Catalog_Name_WB + '.gf_quality.g_audit'
    #Create_Audit_table(Audit_Table_Name)
    #Create_Audit_table_Detail(Catalog)
    
    schema = StructType([
    StructField("Audit_Job_ID",StringType(),True),
    StructField("Audit_Run_ID",StringType(),True),
    StructField("Audit_Start_date_Time",TimestampType(),True),
    StructField("Audit_End_Date_Time",TimestampType(),True),
    StructField("Audit_Source_Name",StringType(),True),
    StructField("Audit_Table_Name",StringType(),True),
    StructField("Audit_Status",StringType(),True),
    StructField("Audit_Count",IntegerType(),True),
    StructField("Audit_Max_ID",IntegerType(),True),
    StructField("Audit_Min_ID",IntegerType(),True),
    StructField("Audit_Error_Message",StringType(),True),
    StructField("L1_Last_Run_Date",TimestampType(),True),
    StructField("G_last_Run_Date",TimestampType(),True)

    ])

    df = spark.createDataFrame([[Audit_Job_ID ,Audit_Run_ID,Audit_Start_date_Time ,Audit_End_Date_Time ,Audit_Source_Name ,Audit_Table_Name ,Audit_Status,Audit_Count,Audit_Max_ID,Audit_Min_ID,Audit_Error_Message,l1_last_run_date,g_last_run_date ] ],schema )
    delta_g_merge_all(Audit_log_table,df,"Audit_Job_ID,Audit_Run_ID" )


    

# COMMAND ----------

# DBTITLE 1,Generic function to get merge key from g_audit_master table
def Find_G_Merge_Key(Catalog_Name_WB,ATName,SName):
    Audit_Table_Name = ATName
    catalog_name=Catalog_Name_WB
    source_Name=SName
    Merge_key_Query="select Audit_Merge_Key from {}.gf_quality.g_audit_master where Audit_Table_Name = '{}' and lower(Audit_Source_Name) = '{}'".format(Catalog_Name_WB,Audit_Table_Name,Source_Name)
    Merge_key = spark.sql(Merge_key_Query).first()["Audit_Merge_Key"]
    return(Merge_key)

# COMMAND ----------

# DBTITLE 1,Generic function to get Audit_Master_ID from g_audit_master table
def Find_G_Audit_Master_ID(Catalog_Name_WB,SName,ATName):
    catalog_name=Catalog_Name_WB
    #Table_Name = TName
    Audit_Table_Name = ATName
    Source_Name=SName
    Audit_Master_ID_Query="select Audit_Master_ID from {}.gf_quality.g_audit_master where Audit_Table_Name = '{}' and lower(Audit_Source_Name) = '{}'".format(catalog_name,Audit_Table_Name,Source_Name)
    Audit_Master_ID = spark.sql(Audit_Master_ID_Query).first()["Audit_Master_ID"] 
    return(Audit_Master_ID)

# COMMAND ----------

# DBTITLE 1,Generic function to get max L1 update date from L1 table
def Find_L1_Water_Mark_DT(Catalog_Name_WB,Water_mark_back_days,SName,TName):
    Source_Name = SName
    Table_Name = TName
    #Catalog_name = Table_Name.split('.')[0]
    Max_water_mark_Query = "SELECT max(L1_UPDATED_DATE)  from {3}".format(Catalog_Name_WB,Water_mark_back_days,Source_Name,Table_Name)
    print(Max_water_mark_Query)
    water_mark_DT=sqlContext.sql(Max_water_mark_Query).first()[0]
    if water_mark_DT is None:
        water_mark_DT = '1900-01-01 01:00:00.00'
    return(water_mark_DT)

# COMMAND ----------

# DBTITLE 1,Generic function to get watermark (G_update_date) from Gold table
def Find_G_Water_Mark_DT(Catalog_Name_WB,Water_mark_back_days,SName,TName):
    Source_Name = SName
    Table_Name = TName
    #Catalog_name = Table_Name.split('.')[0]
    g_last_load_date_Query = "SELECT max(G_UPDATED_DATE)  from {3}".format(Catalog_Name_WB,Water_mark_back_days,Source_Name,Table_Name)
    g_last_load_DT=sqlContext.sql(g_last_load_date_Query).first()[0]
    if g_last_load_DT is None:
        g_last_load_DT = '1900-01-01 01:00:00.00'
    return(g_last_load_DT)
