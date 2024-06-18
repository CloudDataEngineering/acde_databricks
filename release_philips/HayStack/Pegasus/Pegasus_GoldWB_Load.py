# Databricks notebook source
# DBTITLE 0,Libraries
from delta.tables import *
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date, timedelta, datetime
import pandas as pd
from pyspark.sql.functions import concat_ws,col

# COMMAND ----------

# DBTITLE 1,Run  Audit  notebook
# MAGIC %run /Shared/release/HayStack/Common_Functions/Pegasus_G_Audit

# COMMAND ----------

# DBTITLE 1,Parameter from ADF Job
########################################################################################################################
# Audit_Job_ID: Pass from ADF pipline as @pipeline().RunId. Its a unique value for each run                                                 
# ADF_Name : Is ADF name pass from ADf pipline as @pipeline().DataFactory it can be ('az23d1-gf-quality-adf/az23q1-gf-quality-adf/az23t1-gf-quality-adf/az23p1-gf-quality-adf)                                
# Water_mark_back_days: From last load date(water mark date) how many days back this job will run. Right now it is set as -3 by default  
########################################################################################################################

dbutils.widgets.text("Audit_Job_ID", "", "Audit_Job_ID")
Audit_Job_ID = dbutils.widgets.get("Audit_Job_ID")

dbutils.widgets.text("ADF_Name", "", "ADF_Name")
ENV = dbutils.widgets.get("ADF_Name")

dbutils.widgets.text("Water_mark_back_days", "", "Water_mark_back_days")

Water_mark_back_days  = dbutils.widgets.get("Water_mark_back_days")

dbutils.widgets.text("load_type", "", "load_type")
load_type  = dbutils.widgets.get("load_type")

dbutils.widgets.text("Source_Name", "", "Source_Name")
Source_Name  = dbutils.widgets.get("Source_Name")
Source_Name=Source_Name.lower()

dbutils.widgets.text("Source_Schema", "", "Source_Schema")
Source_Schema  = dbutils.widgets.get("Source_Schema")

dbutils.widgets.text("Target_Schema", "", "Target_Schema")
Target_Schema  = dbutils.widgets.get("Target_Schema")

print(Source_Name,Source_Schema,Target_Schema)


# COMMAND ----------

# DBTITLE 1,Environments related
#############################################################################################
# Based on  Environment name i.e. Development, Test, QA,PROD. Catalog name will be set      #
#############################################################################################
if ENV == 'az23d1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_Gold = 'dev_wb'
elif ENV == 'az23q1-gf-quality-adf':
    Catalog_Name_L1 = 'qa_l1'
    Catalog_Name_Gold = 'qa_wb'
elif ENV == 'az23t1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_Gold = 'test_wb'
elif ENV == 'az23p1-gf-quality-adf':
    Catalog_Name_L1 = 'prod_l1'
    Catalog_Name_Gold = 'prod_wb'    
#Source_Name='Pegasus'

# COMMAND ----------

# DBTITLE 1,Create 2 list:  1) Ful load table 2) Inc table
########################################################################################################################
# Crate 2 list 
#==================================================
# Full_load_table :  List of table that need to be truncate  and load 
# Inc_load_table : List of table  for incremental   load 
########################################################################################################################
Master_Table_Name=Catalog_Name_Gold+'.gf_quality.g_audit_master'
Full_load_table="""select Audit_table_name from {0} where lower(Audit_Load_Type) = 'full' and lower(audit_source_name)='{1}'""".format(Master_Table_Name,Source_Name)
df_Full_table_list=spark.sql(Full_load_table)
#display(table_list)
Full_load_table = list(
    df_Full_table_list.select('Audit_table_name').toPandas()['Audit_table_name']
)
print('full table list')
print(Full_load_table)



Inc_load_table="""select Audit_table_name from {0} where lower(Audit_Load_Type) = 'inc' and lower(audit_source_name)='{1}'""".format(Master_Table_Name,Source_Name)
print(Inc_load_table)
df_inc_table_list=spark.sql(Inc_load_table)
#display(table_list)
Inc_load_table = list(
    df_inc_table_list.select('Audit_table_name').toPandas()['Audit_table_name']
)
print('Inc table list')
print(Inc_load_table)

# COMMAND ----------

# DBTITLE 1,load Gold table present in  Full_load_table in a loop
########################################################################################################################
# Generic code to load table present in Full_load_table list  
########################################################################################################################
for i in Full_load_table:
    try: 
        Audit_Start_date_Time = datetime.now()
        Audit_Table_Name=Catalog_Name_Gold+'.'+Target_Schema+'.g_audit'
        Target_Table_Name=Catalog_Name_Gold+'.'+Target_Schema+'.'+i
        Source_Table_Name=Catalog_Name_L1+'.'+Source_Schema+'.'+ i.replace('g_', '', 1)
        #Source_Name='PegasusQDS'
        Audit_Master_ID =Find_G_Audit_Master_ID(Catalog_Name_Gold,Source_Name,i)    
        Audit_G_logging(Catalog_Name_Gold,Audit_Job_ID ,Audit_Master_ID,Audit_Start_date_Time ,None ,Source_Name ,Target_Table_Name ,'Inprogress',None, None,None, None,None, None)
        source_Query="""select * from {0}""".format(Source_Table_Name)
        df_Source=spark.sql(source_Query)
        df_Source_rename=df_Source.withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE").withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")
       
        df_Source_data=df_Source_rename.withColumn('G_LOADED_DATE',lit(Audit_Start_date_Time)).withColumn('G_UPDATED_DATE',lit(Audit_Start_date_Time))
        df_Source_data=df_Source_data.persist()
        var_source_count = df_Source_data.count()
        # if(var_source_count>0 & i[:10].upper()=='g_rds_grid'):
        #     var_source_min_ID=df_Source_data.selectExpr("cast(min(PR_ID) as int)").first()[0]     
        #     var_source_max_ID=df_Source_data.selectExpr("cast(max(PR_ID) as int)").first()[0]
        #     print(var_source_max_ID)
        # else:
        var_source_min_ID=None
        var_source_max_ID=None
        
        Delta_g_OverWrite(Target_Table_Name,df_Source_data ) 
        
        Audit_G_logging(Catalog_Name_Gold,Audit_Job_ID ,Audit_Master_ID,Audit_Start_date_Time ,datetime.now() ,Source_Name  ,Target_Table_Name,'Completed',var_source_count,var_source_max_ID,var_source_min_ID,None,None,None)
        df_Source_data.unpersist()
    except Exception as err:
        Audit_G_logging(Catalog_Name_Gold,Audit_Job_ID ,Audit_Master_ID,Audit_Start_date_Time ,datetime.now() ,Source_Name  ,Target_Table_Name,'Error',None,None,None,err,None,None)
        print(err)
        break

# COMMAND ----------

# DBTITLE 1,load Gold table present in  Inc_load_table in a loop
########################################################################################################################
# Generic code to load table present in Inc_load_table list  
########################################################################################################################
for i in Inc_load_table:
    try: 
        #Set all variable 
        Audit_Start_date_Time = datetime.now()
        Audit_Table_Name=Catalog_Name_Gold+'.'+Target_Schema+'.g_audit'
        Target_Table_Name=Catalog_Name_Gold+'.'+Target_Schema+'.'+i
        Source_Table_Name=Catalog_Name_L1+'.'+Source_Schema+'.'+ i.replace('g_', '', 1)
        #Source_Name='PegasusQDS'
        print(Target_Table_Name)
        Audit_Master_ID =Find_G_Audit_Master_ID(Catalog_Name_Gold,Source_Name,i) 
        Merge_key =Find_G_Merge_Key(Catalog_Name_Gold,i,Source_Name)
        
        if(load_type.upper() == 'FULL'):
            Water_Mark_DT_STR='1900-01-01 01:00:00.00'
            Water_Mark_DT =datetime.strptime('1900-01-01 01:00:00.00', '%Y-%m-%d %H:%M:%S.%f')
            G_Water_Mark_DT=Water_Mark_DT

        else:
            Water_Mark_DT =Find_L1_Water_Mark_DT(Catalog_Name_Gold,0,Source_Name,Target_Table_Name)
            Water_Mark_DT_STR=Water_Mark_DT
            Water_Mark_DT =datetime.strptime(str(Water_Mark_DT), '%Y-%m-%d %H:%M:%S.%f')
            G_Water_Mark_DT =Find_G_Water_Mark_DT(Catalog_Name_Gold,0,Source_Name,Target_Table_Name)
            G_Water_Mark_DT=datetime.strptime(str(G_Water_Mark_DT), '%Y-%m-%d %H:%M:%S.%f')


        #######Inprogress Audit entry befor load start ######
          
        Audit_G_logging(Catalog_Name_Gold,Audit_Job_ID ,Audit_Master_ID,Audit_Start_date_Time ,None ,Source_Name,Target_Table_Name,'Inprogress',None, None,None, None,None,None)
        
        source_Query="""select * from {0} where LAST_UPDATED_DATE >= '{1}'""".format(Source_Table_Name,Water_Mark_DT)
        df_Source=spark.sql(source_Query)
        
        df_Source_rename=df_Source.withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE").withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")
        df_Source_data=df_Source_rename.withColumn('G_LOADED_DATE',lit(Audit_Start_date_Time)).withColumn('G_UPDATED_DATE',lit(Audit_Start_date_Time))
        #df_Source_data=df_Source_data.persist()


        var_source_count = df_Source_data.count()
        var_source_max_ID=None
        var_source_min_ID=None
    
        ####### For first or full load we are doing over 
        print('Water_Mark_DT_STR',Water_Mark_DT_STR,type(Water_Mark_DT_STR),G_Water_Mark_DT,type(G_Water_Mark_DT))
        print('Water_Mark_DT',Water_Mark_DT,type(Water_Mark_DT),G_Water_Mark_DT,type(G_Water_Mark_DT))
        if (Water_Mark_DT_STR == '1900-01-01 01:00:00.00') :
            print('Delta_g_OverWrite')
            Delta_g_OverWrite(Target_Table_Name,df_Source_data)
            print(i,'Delta_g_OverWrite completed')
        else:
            print('delta_g_merge',Water_Mark_DT)
            delta_g_merge(Target_Table_Name,df_Source_data,Merge_key ) 
            print(i,'delta_g_merge completed')
          
        ####### Audit entry after load completed with status as 'Completed' ######
        Audit_G_logging(Catalog_Name_Gold,Audit_Job_ID ,Audit_Master_ID,Audit_Start_date_Time ,datetime.now() ,Source_Name,Target_Table_Name,'Completed',var_source_count,var_source_max_ID,var_source_min_ID,None,Water_Mark_DT,G_Water_Mark_DT)
        #df_Source_data.unpersist()

    except Exception as err:
        Audit_G_logging(Catalog_Name_Gold,Audit_Job_ID,Audit_Master_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name,Target_Table_Name,'Error',None,None,None,err,None,None)
        print(err)
        break
