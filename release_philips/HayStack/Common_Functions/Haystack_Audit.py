# Databricks notebook source
# DBTITLE 1,# import libraries
import json
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType,LongType
import pandas as pd
from pyspark.sql import functions as f
from datetime import datetime
from pyspark.sql import SparkSession


# COMMAND ----------

# MAGIC %run /Shared/release/HayStack/Common_Functions/Haystack_Merge

# COMMAND ----------

def Audit_logging (Audit_Run_ID ,Audit_Job_ID ,Audit_Start_date_Time ,Audit_End_Date_Time ,Audit_Source_Name ,Audit_Table_Name ,Audit_Status,Audit_Count,Audit_Max_ID,Audit_Min_ID,Audit_Error_Message ):

    Catalog = Audit_Table_Name.split('.')[0]
    Audit_log_table = Catalog + '.qnr.l2_audit'
    #Create_Audit_table(Audit_Table_Name)
    #Create_Audit_table_Detail(Catalog)
    
    schema = StructType([
    StructField("Audit_Run_ID",StringType(),True),
    StructField("Audit_Job_ID",StringType(),True),
    StructField("Audit_Start_date_Time",TimestampType(),True),
    StructField("Audit_End_Date_Time",TimestampType(),True),
    StructField("Audit_Source_Name",StringType(),True),
    StructField("Audit_Table_Name",StringType(),True),
    StructField("Audit_Status",StringType(),True),
    StructField("Audit_Count",IntegerType(),True),
    StructField("Audit_Max_ID",IntegerType(),True),
    StructField("Audit_Min_ID",IntegerType(),True),
    StructField("Audit_Error_Message",StringType(),True),
    ])

    df = spark.createDataFrame([[Audit_Run_ID ,Audit_Job_ID ,Audit_Start_date_Time ,Audit_End_Date_Time ,Audit_Source_Name ,Audit_Table_Name ,Audit_Status,Audit_Count,Audit_Max_ID,Audit_Min_ID,Audit_Error_Message ] ],schema )
    df.show()
    delta_merge_all(Audit_log_table,df,"Audit_Job_ID,Audit_Run_ID" )


    

# COMMAND ----------

def Find_Merge_Key(TName,ATName):
    Table_Name = TName
    Audit_Table_Name = ATName
    Merge_key_Query="select Audit_Merge_Key from {}.qnr.l2_audit_master where Audit_Table_Name = '{}' and Audit_Source_Name = '{}'".format(Catalog_Name_L2,Audit_Table_Name,Source_Name)
    Merge_key = spark.sql(Merge_key_Query).first()["Audit_Merge_Key"]
    # print(Merge_key)
    return(Merge_key)

# COMMAND ----------

def Find_Audit_Master_ID(TName,ATName):
    Table_Name = TName
    Audit_Table_Name = ATName
    Audit_Master_ID_Query="select Audit_Master_ID from {}.qnr.l2_audit_master where Audit_Table_Name = '{}' and Audit_Source_Name = '{}'".format(Catalog_Name_L2,Audit_Table_Name,Source_Name)
    Audit_Master_ID = spark.sql(Audit_Master_ID_Query).first()["Audit_Master_ID"] 
    # print(Audit_Master_ID)
    return(Audit_Master_ID)

# COMMAND ----------

def Find_Water_Mark_DT(TName,ATName):
    Table_Name = TName
    Audit_Table_Name = ATName
    Catalog_name = Table_Name.split('.')[0]
    Max_water_mark_Query = "SELECT date_add(max(Audit_End_Date_Time) ,{}) from {}.qnr.l2_audit WHERE Audit_Source_Name='{}' and lower(Audit_Table_Name) ='{}'".format(Water_mark_back_days,Catalog_Name_L2,Source_Name,Table_Name)
    water_mark_DT=sqlContext.sql(Max_water_mark_Query).first()[0]
    # print(water_mark_DT)
    if water_mark_DT is None:
        water_mark_DT = '1900-01-01 01:00:00.00'
    # print(water_mark_DT)
    return(water_mark_DT)
