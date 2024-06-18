# Databricks notebook source
# DBTITLE 1,Library
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
# MAGIC #### Set Parameters

# COMMAND ----------

dbutils.widgets.text("SourceName", "", "SourceName")
dbutils.widgets.text("ADF_Name", "", "ADF_Name")
dbutils.widgets.text("ADF_JobID", "", "ADF_JobID")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Parameters

# COMMAND ----------

System_SourceName = dbutils.widgets.get("SourceName")
Adf_Name = dbutils.widgets.get("ADF_Name")
ADF_JobID = dbutils.widgets.get("ADF_JobID")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set ENV variable

# COMMAND ----------

#############################################################################################
# Based on  Environment name i.e. Development, Test, QA,PROD. Catalog name will be set      #
#############################################################################################
if Adf_Name == 'az23d1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'dev_l2'
elif Adf_Name == 'az23q1-gf-quality-adf':
    Catalog_Name_L1 = 'qa_l1'
    Catalog_Name_L2 = 'qa_l2'
elif Adf_Name == 'az23t1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'test_l2'
elif Adf_Name == 'az23p1-gf-quality-adf':
    Catalog_Name_L1 = 'prod_l1'
    Catalog_Name_L2 = 'prod_l2'    
Source_Name='CatsWeb'


# COMMAND ----------

# MAGIC %md
# MAGIC ###SET LOOP Variable - List DQ_ID

# COMMAND ----------

pd_Max_Value=spark.sql("select DQ_ID from {0}.qnr.l2_audit_dq_monitoring_rules where Source_Schema = '{1}' order by DQ_ID".format(Catalog_Name_L2,Source_Name)).toPandas()['DQ_ID']
Max_Value_list=list(pd_Max_Value)
print(Max_Value_list)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Validate all Data based on l2_audit_dq_monitoring_rules

# COMMAND ----------

Run_Date= datetime.now()
Run_Date_st="'" + str(Run_Date) + "'"
Result_dict={}
for i in Max_Value_list:
            
            print(i)
            Result_status=''
            # featch DQ Rule from l2_audit_dq_monitoring_rules
            Audit_df=spark.sql("select * from {2}.qnr.l2_audit_dq_monitoring_rules where  DQ_ID = {0} and Source_Name = {1}".format(i,"'" + System_SourceName + "'",Catalog_Name_L2 ))
            Audit_df.show()

            #SET PARAMETER 
            Count_Silver_L2 ='N/A'
            Count_Silver_L1='N/A'
            DQ_ID=Audit_df.select('DQ_ID').collect()[0][0]
            Source_Schema=Audit_df.select('Source_Schema').collect()[0][0]
            Source_Table=Audit_df.select('Source_Table').collect()[0][0]
            Source_Filter=Audit_df.select('Source_Filter').collect()[0][0]
            Source_Columns=Audit_df.select('Source_Columns').collect()[0][0]
            Target_Schema=Audit_df.select('Target_Schema').collect()[0][0]
            Target_Table_Name=Audit_df.select('Target_Table_Name').collect()[0][0]
            Target_Filter=Audit_df.select('Target_Filter').collect()[0][0]
            Target_Columns=Audit_df.select('Target_Columns').collect()[0][0]
            Source_Name=Audit_df.select('Source_Name').collect()[0][0]
            ExtractionType=Audit_df.select('ExtractionType').collect()[0][0]
            Result_dict_key=Source_Name+'_'+Target_Table_Name +'_Validation_'+str(i)
            
            #Create source and target query for Data validation for each rule  
            
            df_Source_catalog=spark.sql("Use CATALOG {}".format(Catalog_Name_L1))
            query_sql=" select {0} from {2} where {3}  ;".format(Source_Columns,Source_Schema,Source_Table,Source_Filter)
            df_Source_value=spark.sql(query_sql)


            df_Target_value= spark.sql("select {} from {}.{}.{} where {}".format(Target_Columns,Catalog_Name_L2,Target_Schema,Target_Table_Name,Target_Filter))

            #Compare source and target DF
            df_result_SL1_SL2=df_Source_value.subtract(df_Target_value)
            df_result_SL2_SL1=df_Target_value.subtract(df_Source_value)


            #Add Count to variable 
            if Target_Columns.upper() =='COUNT(*)':
                Count_Silver_L2=df_Target_value.select(df_Target_value.columns[0:1]).collect()[0][0]
                Count_Silver_L1=df_Source_value.select(df_Source_value.columns[0:1]).collect()[0][0]
                Count_Silver_L2=str(Count_Silver_L2)
                Count_Silver_L1=str(Count_Silver_L1)

            #Insert result log based on data Validation  is PASS or FAIL
            
            if (df_result_SL2_SL1.count() == 0 and df_result_SL1_SL2.count() == 0 ):
                Result_status='Success' 
                print(Result_status)
                Query= "insert into {0}.qnr.l2_audit_dq_monitoring_rule_log (ADF_JobID,Run_Date,DQ_ID,Count_Silver_L1,Count_Silver_L2,Result_SL1_SL2) values ({1} ,{2} ,{3}  ,{4} ,{5} ,{6} )".format( Catalog_Name_L2 ,"'" + ADF_JobID +"'",  Run_Date_st , DQ_ID   ,"'" + str(Count_Silver_L1)  +"'"  , "'" + str(Count_Silver_L2) +"'" , "'" + Result_status +"'"  )        
                spark.sql(Query)
                Result_dict[Result_dict_key]=Result_status
            else:
                Result_status='Failed'

                print(Result_status)
                Query= "insert into {0}.qnr.l2_audit_dq_monitoring_rule_log (ADF_JobID,Run_Date,DQ_ID,Count_Silver_L1,Count_Silver_L2,Result_SL1_SL2) values ({1} ,{2} ,{3}  ,{4} ,{5},{6}   )".format( Catalog_Name_L2 ,"'" + ADF_JobID +"'",  Run_Date_st , DQ_ID   ,"'" + str(Count_Silver_L1)  +"'"  , "'" + str(Count_Silver_L2)  +"'", "'"+ Result_status +"'"  )         
                spark.sql(Query)
                Result_dict[Result_dict_key]=Result_status

#Pass list to ADF job for email notification
Result_list=list(Result_dict.items())
print(Result_list)
dbutils.notebook.exit(Result_list)  

# COMMAND ----------


