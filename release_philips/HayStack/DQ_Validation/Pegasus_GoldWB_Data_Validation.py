# Databricks notebook source
# DBTITLE 1,Library
from delta.tables import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, timedelta, datetime

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Parameters

# COMMAND ----------

dbutils.widgets.text("SourceName", "", "SourceName")


dbutils.widgets.text("ADF_Name", "", "ADF_Name")
dbutils.widgets.text("ADF_JobID", "", "ADF_JobID")
dbutils.widgets.text("Load_Type", "", "Load_Type")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Parameters

# COMMAND ----------

SourceName = dbutils.widgets.get("SourceName")
#System_SourceName = SourceName+'QDS'
Adf_Name = dbutils.widgets.get("ADF_Name")
ADF_JobID = dbutils.widgets.get("ADF_JobID")
Load_Type = dbutils.widgets.get("Load_Type")
Source_Schema='pegasus'
l2_schema_name ='gf_quality'

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
    Catalog_Name_wb = 'dev_wb'
elif Adf_Name == 'az23q1-gf-quality-adf':
    Catalog_Name_L1 = 'qa_l1'
    Catalog_Name_L2 = 'qa_l2'
    Catalog_Name_wb = 'qa_wb'
elif Adf_Name == 'az23t1-gf-quality-adf':
    Catalog_Name_L1 = 'test_l1'
    Catalog_Name_L2 = 'test_l2'
    Catalog_Name_wb = 'test_wb'
elif Adf_Name == 'az23p1-gf-quality-adf':
    Catalog_Name_L1 = 'prod_l1'
    Catalog_Name_L2 = 'prod_l2'  
    Catalog_Name_wb = 'prod_wb'  
#Source_Name='pegasus'
# schema_name =


# COMMAND ----------

# MAGIC %md
# MAGIC ###SET LOOP Variable - List DQ_ID

# COMMAND ----------

tablist=spark.sql("select Audit_table_name from {1}.gf_quality.g_audit_master where audit_source_name = '{0}'".format(SourceName,Catalog_Name_wb)).toPandas()['Audit_table_name']
total_table_list=list(tablist)
print(total_table_list)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Validate Data for all tables

# COMMAND ----------

Run_Date= datetime.now()
Run_Date_st="'" + str(Run_Date) + "'"
Result_dict={}

for tablename in total_table_list:
            print(tablename)
            Result_status=''
            # featch DQ Rule from l2_audit_dq_monitoring_rules
            audit_query ="""select * from {2}.{3}.g_audit_master where  Audit_table_name = '{0}' and Audit_Source_Name = {1}""".format(tablename,"'" + SourceName + "'",Catalog_Name_wb,l2_schema_name)
            print(audit_query)
            Audit_df=spark.sql(audit_query)
            Audit_df.show()
            # watermarkcolumn=Audit_df.select('Audit_Update_Key').collect()[0][0]
            watermarkcolumn_l1= 'LAST_UPDATED_DATE'
            watermarkcolumn_G = 'G_UPDATED_DATE'
            
            audit_table_name = Catalog_Name_wb +'.'+ l2_schema_name+'.' + tablename

            watermarkdate_l1_query = """select max(L1_Last_Run_Date) as L1_Last_Run_Date from {0}.{1}.g_audit where Audit_Table_Name = '{2}'""".format(Catalog_Name_wb,l2_schema_name,audit_table_name)
            print(watermarkdate_l1_query)
            watermarkdate_l1 = spark.sql(watermarkdate_l1_query).collect()[0][0]
            print(watermarkdate_l1)

            watermarkdate_G = spark.sql("""select max(G_Last_Run_Date) as G_Last_Run_Date from {0}.{1}.g_audit where Audit_Table_Name = '{2}'""".format(Catalog_Name_wb,l2_schema_name,audit_table_name)).collect()[0][0]
            print(watermarkdate_G)

            if(Load_Type.lower() =='inc'):
                Audit_Load_Type=Audit_df.select('Audit_Load_Type').collect()[0][0]
            else:
                Audit_Load_Type ='full'            
            Audit_Master_Id=Audit_df.select('Audit_Master_ID').collect()[0][0]

            if(Audit_Load_Type.lower() =='full'):
                watermarkcolumn_l1 =0
                watermarkcolumn_G =0
                watermarkdate_l1 =0
                watermarkdate_G =0

            print(Audit_Load_Type)
            Result_dict_key=Source_Schema+'_'+tablename +'_Validation:'
            target_tablename =tablename
            length_table_name =len(tablename)
            # print(length_table_name)
            source_tablename =tablename[2:]

            tablename_columns =spark.sql("""select * from {0}.{1}.{2} where 1=0""".format(Catalog_Name_wb,l2_schema_name,target_tablename))

            column_list = [each_column for each_column in tablename_columns.columns if each_column not in ('ADLS_LOADED_DATE','LAST_UPDATED_DATE','L1_LOADED_DATE','L1_UPDATED_DATE','G_LOADED_DATE','G_UPDATED_DATE')]
            column_list_new = ','.join(column_list)


            source_df_query ="""select {0} from {1}.{2}.{3} where {4}>='{5}' """.format(column_list_new,Catalog_Name_L1,Source_Schema,source_tablename,watermarkcolumn_l1,watermarkdate_l1)
            print(source_df_query)
            source_df = spark.sql(source_df_query)

            target_df_query ="""select {0} from {1}.{2}.{3} where {4}>='{5}' """.format(column_list_new,Catalog_Name_wb,l2_schema_name,target_tablename,watermarkcolumn_G,watermarkdate_G)
            print(target_df_query)
            target_df = spark.sql(target_df_query)

            source_df_count =source_df.count()
            target_df_count = target_df.count()
            print(source_df_count)
            print(target_df_count)
            if(source_df_count==target_df_count):
                df_source_target = source_df.subtract(target_df)
                if(df_source_target.count()>0):
                    Result_status='Failed'
                else:
                    Result_status='Success'
            else:
                Result_status='Failed'
            print(Result_status)
            Result_dict[Result_dict_key]=Result_status
    

            #Insert result log based on data Validation  is PASS or FAIL


            print(Result_status)
            Query= "insert into {0}.{7}.g_audit_dq_monitoring_rule_log (ADF_JobID,Run_Date,DQ_ID,Count_Silver_L1,Count_Silver_G,Result_SL1_G) values ({1} ,{2} ,{3}  ,{4} ,{5} ,{6} )".format( Catalog_Name_wb ,"'" + ADF_JobID +"'",  Run_Date_st , Audit_Master_Id   ,"'" + str(source_df_count)  +"'"  , "'" + str(target_df_count) +"'" , "'" + Result_status +"'" ,l2_schema_name) 
            print(Query)
            spark.sql(Query)
            Result_dict[Result_dict_key]=Result_status

#Pass list to ADF job for email notification
filter_string = 'Failed'
filtered_dict = {k:v for (k,v) in Result_dict.items() if filter_string in v}
Result_list=list(filtered_dict.items())
print(Result_list)
dbutils.notebook.exit(Result_list)  
