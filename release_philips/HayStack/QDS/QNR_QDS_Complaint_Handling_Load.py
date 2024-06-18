# Databricks notebook source
# DBTITLE 1,Libraries
from delta.tables import *
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date, timedelta, datetime
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Run  Audit  notebook
# MAGIC %run /Shared/release/HayStack/Common_Functions/Haystack_Audit

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

# COMMAND ----------

# DBTITLE 1,Environments related
#############################################################################################
# Based on  Environment name i.e. Development, Test, QA,PROD. Catalog name will be set      #
#############################################################################################
if ENV == 'az23d1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'dev_l2'
elif ENV == 'az23q1-gf-quality-adf':
    Catalog_Name_L1 = 'qa_l1'
    Catalog_Name_L2 = 'qa_l2'
elif ENV == 'az23t1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'test_l2'
elif ENV == 'az23p1-gf-quality-adf':
    Catalog_Name_L1 = 'prod_l1'
    Catalog_Name_L2 = 'prod_l2'    
Source_Name='QDS'

# COMMAND ----------

# MAGIC %md # Load COMPLAINT Table

# COMMAND ----------

################################################################################################################
# Find_Merge_Key  Function: Used to find the merge key for delta merge for the complaint table.                #
# Find_Audit_Master_ID Function: Used to find Audit Master ID from Audit master table for Audit entry          #
# Find_Water_Mark_DT Function: Used to Last load data (watermark  date)                                        #
# Note: Watermark column will be subtracked from Water_mark_back_days                                          #
################################################################################################################

Table_Name=Catalog_Name_L2+'.qnr.complaint'
Audit_Table_Name='COMPLAINT'
Merge_key =Find_Merge_Key(Table_Name,Audit_Table_Name)
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)
Water_Mark_DT =Find_Water_Mark_DT(Table_Name,Audit_Table_Name)
print(Water_Mark_DT)


# COMMAND ----------

# DBTITLE 1,Create Source Query Data frame
Complaint_Query="""select 
    cast(PR1.ID as int) as COMPLAINT_ID ,
    17973 as APPLICATION_ID,
    PR_S.NAME as COMPLAINT_STATE,
    cast(DF_530 as DATE) as EVENT_DATE ,
    PR1.DF_3 as DATE_OPENED,
    ADT_379.Name as PRODUCT_FAMILY ,
    ADT_84.Name as OWNING_ENTITY_NAME ,
    DF_1157 as CATALOG_ITEM_ID ,
    DF_1162 as CATALOG_ITEM_NAME ,
    DF_1137 as TRADE_ITEM_ID ,
    DF_1154 as TRADE_ITEM_NAME ,
    DF_574 as DEVICE_IDENTIFIER ,
    case when lower(trim(ADT_1286.NAME))='no' then 'N' when lower(trim(ADT_1286.NAME))='yes' THEN 'y' ELSE NULL end as IS_THIRD_PARTY_DEVICE , 
    DF_857 as MODEL_NUMBER ,
    case when lower(trim(ADT_1156.NAME))='no' THEN 'N' when  lower(trim(ADT_1156.NAME))='yes' THEN 'Y'  else NULL END as IS_MEDICAL_DEVICE ,
    DF_554 as PRODUCT_UDI ,
    DF_534 as MANUFACTURE_DATE ,
    DF_543 as SHIP_DATE ,
    DF_531 as EXPIRATION_DATE ,
    ADT_635.NAME  as REPORTER_COUNTRY ,
    ADT_662.NAME as EVENT_COUNTRY ,
    DF_535 as PHILIPS_NOTIFIED_DATE ,
    DF_533 as INITIATE_DATE ,
    DF_512 as BECOME_AWARE_DATE ,
    PR1.DF_0 as COMPLAINT_SHORT_DESCRIPTION ,
    DF_398 as COMPLAINT_CUSTOMER_DESCRIPTION ,
    ADT_1249.NAME as REPORTED_PROBLEM_CODE_L1,
    ADT_1250.NAME as REPORTED_PROBLEM_CODE_L2,
    ADT_1366.NAME as REPORTED_PROBLEM_CODE_L3,
    ADT_460.NAME as DEVICE_USE_AT_TIME_OF_EVENT ,
    case when lower(trim(ADT_619.NAME))='no' THEN 'N' WHEN lower(trim(ADT_619.NAME)) ='yes' THEN 'Y' ELSE NULL END as IS_PATIENT_USER_HARMED ,
    case when lower(trim(ADT_624.NAME))='no' THEN 'N' when lower(trim(ADT_624.NAME))='yes' then 'Y' ELSE NULL END as IS_POTENTIAL_SAFETY_ALERT,
    case when lower(trim(ADT_625.NAME))='no' THEN 'N' when lower(trim(ADT_625.NAME))='yes' THEN 'Y' ELSE NULL end as IS_POTENTIAL_SAFETY_EVENT,
    case when lower(trim(ADT_1368.NAME))='no' THEN 'N' when lower(trim(ADT_1368.NAME))='yes' THEN 'Y' ELSE NULL end as IS_ALLEGATION_OF_INJURY_OR_DEATH,
	case when lower(trim(ADT_450.NAME))='no' THEN 'N' when lower(trim(ADT_450.NAME))='yes' THEN 'Y' ELSE NULL end as  HAS_DEVICE_ALARMED,
    ADT_1369.NAME as INCIDENT_KEY_WORDS,
    ADT_654.NAME as TYPE_OF_REPORTED_COMPLAINT,
    ADT_1052.NAME as HAZARDOUS_SITUATION,
    DF_72 as COMPLAINT_LONG_DESCRIPTION ,
    DF_399 as SOURCE_NOTES ,
    DF_122 as COMMENTS ,
    DF_392 as INVESTIGATION_SUMMARY ,
    DF_391 as INVESTIGATION_NOTES ,
    ADT_1371.NAME as PROBLEM_SOURCE_CODE,
    ADT_1372.NAME as PROBLEM_REASON_CODE,
    case when lower(trim(ADT_1293.NAME))='no' THEN 'N'   when lower(trim(ADT_1293.NAME))='yes' THEN 'Y' ELSE NULL end as IS_CAPA_ADDTL_INVEST_REQUIRED,
    DF_393 as OTHER_RELEVANT_EVENT_INFO ,
    DF_1095 as PATIENT_ACTIONS_TAKEN ,
    DF_563 as PROBLEM_SYMPTOMS_AND_FREQUENCY ,
    DF_496 as ADDTL_INFORMATION,
    DF_1388 as MEDICAL_RATIONALE,
    ADT_649.NAME as SOURCE_SYSTEM,
    ADT_646.NAME as SOURCE_OF_COMPLAINT,
    PR1.DF_493 as SOURCE_SERIAL_NUMBER,
    PR1.DF_489 as SERIAL_NUMBER, 
    PR1.DATE_CREATED as DATE_CREATED,
    PR1.DATE_CLOSED as  DATE_CLOSED,
    PR1.DF_1159 as SOURCE_CATALOG_ITEM_ID,
    PR1.DF_1163 as SOURCE_CATALOG_ITEM_NAME,
    PR1.DF_593 as LOT_OR_BATCH_NUMBER,
    ADT_645.NAME as SOLUTION_FOR_THE_CUSTOMER,
    PR1.DF_557 as CUSTOMER_NUMBER,
    PR1.DF_476 as NO_FURTHER_INVEST_REQ_ON,
    ADT_1256.NAME as SOURCE_EVENT_COUNTRY,
    NULL as ADDITIONAL_EVALUATION_COMMENTS,
    NULL as SYMPTOM_CODE_1,
    NULL as SYMPTOM_CODE_2,
    NULL as SYMPTOM_CODE_3,
    '{1}' as Audit_Job_ID,
    PR1.ADLS_LOADED_DATE as L1_LOADED_DATE,
    PR1.LAST_UPDATED_DATE as L1_UPDATED_DATE,
    getdate() as  L2_LOADED_DATE,
    getdate() as L2_UPDATED_DATE
    from {0}.qds.vw_rds_pr_1 PR1 LEFT JOIN {0}.qds.vw_rds_pr_2 PR2
    ON PR1.ID=PR2.ID
    Left join {0}.qds.vw_pr_status_type as PR_S
    on PR1.Status_Type= PR_S.ID 
    Left join {0}.qds.vw_addtl_type ADT_379
      on ADT_379.ID = PR1.DF_379
    Left join {0}.qds.vw_addtl_type ADT_84
      on  ADT_84.ID = PR1.DF_84  
    Left join {0}.qds.vw_addtl_type ADT_1286
      on  ADT_1286.ID = PR2.DF_1286  
    Left join {0}.qds.vw_addtl_type ADT_1156
      on  ADT_1156.ID = PR1.DF_1156  
    Left join {0}.qds.vw_addtl_type ADT_635
      on  ADT_635.ID = PR1.DF_635 
    Left join {0}.qds.vw_addtl_type ADT_662
      on  ADT_662.ID = PR1.DF_662
    Left join {0}.qds.vw_addtl_type ADT_1249
      on  ADT_1249.ID = PR2.DF_1249
    Left join {0}.qds.vw_addtl_type ADT_1250
      on ADT_1250.ID = PR2.DF_1250
    Left join {0}.qds.vw_addtl_type ADT_1366
      on  ADT_1366.ID = PR2.DF_1366
    Left join {0}.qds.vw_addtl_type ADT_460
      on  ADT_460.ID = PR1.DF_460
    Left join {0}.qds.vw_addtl_type ADT_619
      on  ADT_619.ID = PR1.DF_619
    Left join {0}.qds.vw_addtl_type ADT_624
      on  ADT_624.ID = PR1.DF_624
    Left join {0}.qds.vw_addtl_type ADT_625
      on  ADT_625.ID = PR1.DF_625
    Left join {0}.qds.vw_addtl_type ADT_1368
      on ADT_1368.ID = PR2.DF_1368  
    Left join {0}.qds.vw_addtl_type ADT_450
      on  ADT_450.ID = PR1.DF_450   
    Left join {0}.qds.vw_addtl_type ADT_1369
      on  ADT_1369.ID = PR2.DF_1369
    Left join {0}.qds.vw_addtl_type ADT_654
      on  ADT_654.ID = PR1.DF_654
	  Left join {0}.qds.vw_addtl_type ADT_1052
      on ADT_1052.ID = PR1.DF_1052
    Left join {0}.qds.vw_addtl_type ADT_1371
      on  ADT_1371.ID = PR2.DF_1371
    Left join {0}.qds.vw_addtl_type ADT_1372
      on ADT_1372.ID = PR2.DF_1372
    Left join {0}.qds.vw_addtl_type ADT_1293  
      on  ADT_1293.ID = PR2.DF_1293   
    Left join {0}.qds.vw_addtl_type ADT_649  
      on  ADT_649.ID = PR1.DF_649 
  Left join {0}.qds.vw_addtl_type ADT_646  
      on  ADT_646.ID = PR1.DF_646    
  Left join {0}.qds.vw_addtl_type ADT_645  
      on  ADT_645.ID = PR1.DF_645    
  Left join {0}.qds.vw_addtl_type ADT_1256  
      on  ADT_1256.ID = PR2.DF_1256 
    WHERE PR1.PROJECT_ID IN (16,17)
    and ( PR1.LAST_UPDATED_DATE >= '{2}'   or PR2.LAST_UPDATED_DATE >= '{2}'  )""".format(Catalog_Name_L1,Audit_Job_ID,Water_Mark_DT)
df_Complaint = spark.sql(Complaint_Query)
df_Complaint.persist()
print(df_Complaint.count())
#df_Complaint.show()

# COMMAND ----------

# DBTITLE 1,Scrubbing
email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
name_regex = r"(Prof.\sdr.\s\w+|Prof.dr.\s\w+|Prof.dr.\w+|Prof.\sdr.\w+|Prof.\sDr.\s\w+|Prof.Dr.\s\w+|Prof.Dr.\w+|Prof.\sDr.\w+|Dr\.\s\w+|Mr\.\s\w+|Dr\.\w+|Mr\.\w+|Prof\.\w+|Prof\.\s\w+|Ms\.\w+|Ms\.\s\w+|Mrs\.\w+|Mrs\.\s\w+|Miss\s\w+|Herr\s\w+|Fräulein\s\w+|Frau\s\w+|Mr./Mrs.\s\w+|Mr./Mrs.\w+|Mr./Ms.\w+|Mr./Ms.\s\w+)"
#phone_regex = r"(\+\d{4,18})"
phone_regex =r"(\+\d{6,18})|(\+\d{1,3}\s\d{3}[-]\d{3}[-]\d{3,4})|(\+\d{1,3}\s[(]\d{3}[)]\s\d{3}[-]\d{3,4})|(\+\d{1,3}[-]\d{6,9})|(\+\d{1,3}[-]\d{2,5}\s\d{3,5})|(\+\d{4}[-]\d{3}[-]\d{4})|(\+\d{1,3}[-]\d{2,3}[-]\d{2,3}[-]\d{2,5})|(\+\d{3}[-]\d{3}[-]\d{4})|(\+\d{3}[-]\d{1}[-]\d{4}[-]\d{3})|(\+\d{3}[-]\d{2}[-]\d{7})|(\+\d{3}[-]\d{3}[-]\d{5})|(\+\d{1,3})"

df_Complaint_scrubbed=df_Complaint.select("COMPLAINT_ID"
,"APPLICATION_ID"
,"COMPLAINT_STATE"
,"EVENT_DATE"
,"DATE_OPENED"
,"PRODUCT_FAMILY"
,"OWNING_ENTITY_NAME"
,"CATALOG_ITEM_ID"
,"CATALOG_ITEM_NAME"           
,"TRADE_ITEM_ID"
,"TRADE_ITEM_NAME"
,"DEVICE_IDENTIFIER"
,"IS_THIRD_PARTY_DEVICE"
,"MODEL_NUMBER"
,"IS_MEDICAL_DEVICE"
,"PRODUCT_UDI"
,"MANUFACTURE_DATE"
,"SHIP_DATE"
,"EXPIRATION_DATE"
,"REPORTER_COUNTRY"
,"EVENT_COUNTRY"
,"PHILIPS_NOTIFIED_DATE"
,"INITIATE_DATE"
,"BECOME_AWARE_DATE"
,regexp_replace(regexp_replace(regexp_replace("COMPLAINT_SHORT_DESCRIPTION", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("COMPLAINT_SHORT_DESCRIPTION")
,regexp_replace(regexp_replace(regexp_replace("COMPLAINT_CUSTOMER_DESCRIPTION", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("COMPLAINT_CUSTOMER_DESCRIPTION")
,"REPORTED_PROBLEM_CODE_L1"
,"REPORTED_PROBLEM_CODE_L2"
,"REPORTED_PROBLEM_CODE_L3"
,"DEVICE_USE_AT_TIME_OF_EVENT"
,"IS_PATIENT_USER_HARMED"
,"IS_POTENTIAL_SAFETY_ALERT"
,"IS_POTENTIAL_SAFETY_EVENT"
,"IS_ALLEGATION_OF_INJURY_OR_DEATH"
,"HAS_DEVICE_ALARMED"
,"INCIDENT_KEY_WORDS"
,"TYPE_OF_REPORTED_COMPLAINT"
,"HAZARDOUS_SITUATION"
,regexp_replace(regexp_replace(regexp_replace("COMPLAINT_LONG_DESCRIPTION", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("COMPLAINT_LONG_DESCRIPTION")
,regexp_replace(regexp_replace(regexp_replace("SOURCE_NOTES", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("SOURCE_NOTES")
,regexp_replace(regexp_replace(regexp_replace("COMMENTS", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("COMMENTS")
,regexp_replace(regexp_replace(regexp_replace("INVESTIGATION_SUMMARY", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("INVESTIGATION_SUMMARY")
,regexp_replace(regexp_replace(regexp_replace("INVESTIGATION_NOTES", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("INVESTIGATION_NOTES")
,"PROBLEM_SOURCE_CODE"
,"PROBLEM_REASON_CODE"
,"IS_CAPA_ADDTL_INVEST_REQUIRED"
,regexp_replace(regexp_replace(regexp_replace("OTHER_RELEVANT_EVENT_INFO", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("OTHER_RELEVANT_EVENT_INFO")
,regexp_replace(regexp_replace(regexp_replace("PATIENT_ACTIONS_TAKEN", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("PATIENT_ACTIONS_TAKEN")
,regexp_replace(regexp_replace(regexp_replace("PROBLEM_SYMPTOMS_AND_FREQUENCY", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("PROBLEM_SYMPTOMS_AND_FREQUENCY")
,regexp_replace(regexp_replace(regexp_replace("ADDTL_INFORMATION", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("ADDTL_INFORMATION")
,regexp_replace(regexp_replace(regexp_replace("MEDICAL_RATIONALE", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("MEDICAL_RATIONALE")
,"SOURCE_SYSTEM"
,"SOURCE_OF_COMPLAINT"
,"SOURCE_SERIAL_NUMBER"
,"SERIAL_NUMBER"
,"DATE_CREATED"
,"DATE_CLOSED"
,"SOURCE_CATALOG_ITEM_ID"
,"SOURCE_CATALOG_ITEM_NAME"
,"LOT_OR_BATCH_NUMBER"
,"SOLUTION_FOR_THE_CUSTOMER"
,"CUSTOMER_NUMBER"
,"NO_FURTHER_INVEST_REQ_ON"
,"SOURCE_EVENT_COUNTRY"
,"ADDITIONAL_EVALUATION_COMMENTS"
,"SYMPTOM_CODE_1"
,"SYMPTOM_CODE_2"
,"SYMPTOM_CODE_3"
,"Audit_Job_ID"
,"L1_LOADED_DATE"
,"L1_UPDATED_DATE"
,"L2_LOADED_DATE"
,"L2_UPDATED_DATE")
#df_Complaint_scrubbing_final = spark.sql(df_Complaint_scrubbed)
df_Complaint_scrubbed.persist()
df_Complaint.unpersist()


# COMMAND ----------

# DBTITLE 1,Merge Target table with Source Query Data frame 
############################################################################################################################
# Step 1: Enter in L2_Audit table for process start                                                                        #
# Step 2: Merage Complaint table based on merge key set above                                                              #
# Step 3a: update Audit table entry  by setting as status Completed                                                        #
# Step 3b: In case of failure update  the Audit table entry  by updating the status as Error with an error message         # 
############################################################################################################################
try: 
    Audit_Start_date_Time = datetime.now()
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,None ,Source_Name ,Table_Name ,'Inprogress',None, None,None, None)
    var_Complaint_count = df_Complaint_scrubbed.count()
    var_Complaint_ID_min=df_Complaint_scrubbed.agg({'COMPLAINT_ID': 'min'}).collect()[0][0]
    var_Complaint_ID_max=df_Complaint_scrubbed.agg({'COMPLAINT_ID': 'max'}).collect()[0][0]
    delta_merge(Table_Name,df_Complaint_scrubbed,Merge_key )
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Completed',var_Complaint_count,var_Complaint_ID_max,var_Complaint_ID_min,None)
    df_Complaint_scrubbed.unpersist()
except Exception as err:
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Error',None,None,None,err)

# COMMAND ----------

# MAGIC %md #COMPLAINT_FAILURE

# COMMAND ----------

################################################################################################################
# Find_Audit_Master_ID Function: Used to find Audit Master ID from Audit master table for Audit entry          #
################################################################################################################

Table_Name=Catalog_Name_L2+'.qnr.complaint_failure'
Audit_Table_Name='COMPLAINT_FAILURE'
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)


# COMMAND ----------

COMPLAINT_FAILURE_Query="""
select
	cast(GRD.PR_ID as int) as COMPLAINT_ID ,
	17973 as APPLICATION_ID,
	GRD.SEQ_NO as SEQ_NO ,
	ADT_1148.Name as FAILURE_CODE,
	ADT_1150.Name as FAILURE_CODE_DETAIL,
	ADT_1151.Name as FAILURE_MONITORING_CODE,
	'{1}' as Audit_Job_ID,
	GRD.ADLS_LOADED_DATE as L1_LOADED_DATE,
	GRD.LAST_UPDATED_DATE as L1_UPDATED_DATE,
	getdate() as  L2_LOADED_DATE,
	getdate() as L2_UPDATED_DATE
from  {0}.qds.vw_rds_pr_1 PR1
Inner join {0}.qds.vw_rds_grid_1144 GRD 
	ON PR1.ID=GRD.PR_ID 
Left join  {0}.qds.vw_addtl_type ADT_1148
	ON GRD.DF_1148 = ADT_1148.ID
Left join  {0}.qds.vw_addtl_type ADT_1150
	ON GRD.DF_1150 = ADT_1150.ID
Left join  {0}.qds.vw_addtl_type ADT_1151
	on GRD.DF_1151 = ADT_1151.ID 
 where PR1.PROJECT_ID IN (16,17) """.format(Catalog_Name_L1,Audit_Job_ID )



# COMMAND ----------

############################################################################################################################
# Step 1: Enter in L2_Audit table for process start                                                                        #
# Step 2: Overwrite complaint_failure table                                                                                #
# Step 3a: Update Audit table entry  by setting as status Completed                                                        #
# Step 3b: In case of failure update  the Audit table entry  by updating the status as Error with an error message         #
############################################################################################################################
try: 
    Audit_Start_date_Time = datetime.now()
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,None ,Source_Name ,Table_Name ,'Inprogress',None, None,None, None)
    df_COMPLAINT_FAILURE=sqlContext.sql(COMPLAINT_FAILURE_Query)
    var_COMPLAINT_FAILURE_count = df_COMPLAINT_FAILURE.count()
    var_Complaint_ID_min=df_COMPLAINT_FAILURE.agg({'COMPLAINT_ID': 'min'}).collect()[0][0]
    var_Complaint_ID_max=df_COMPLAINT_FAILURE.agg({'COMPLAINT_ID': 'max'}).collect()[0][0]
    Delta_OverWrite(Table_Name,df_COMPLAINT_FAILURE )
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Completed',var_COMPLAINT_FAILURE_count,var_Complaint_ID_max,var_Complaint_ID_min,None)
except Exception as err:
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Error', None,None, None,err)

# COMMAND ----------

# MAGIC %md #COMPLAINT_HAZARD Code

# COMMAND ----------

################################################################################################################
# Find_Audit_Master_ID Function: Used to find Audit Master ID from Audit master table for Audit entry          #
################################################################################################################
Table_Name=Catalog_Name_L2+'.qnr.complaint_hazard'
Audit_Table_Name='COMPLAINT_HAZARD'
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)

# COMMAND ----------

Complaint_Hazard_Query="""
select
	GRD.PR_ID as COMPLAINT_ID ,
	17973 as APPLICATION_ID,
	GRD.SEQ_NO as SEQ_NO ,
	ADT_1134.Name as HAZARD_CATEGORY,
	ADT_1133.Name as HAZARD,
	'{1}'  as Audit_Job_ID,
	GRD.ADLS_LOADED_DATE as L1_LOADED_DATE,
	GRD.LAST_UPDATED_DATE as L1_UPDATED_DATE,
	getdate() as  L2_LOADED_DATE,
	getdate() as L2_UPDATED_DATE
from {0}.qds.vw_rds_pr_1 PR1
Inner join {0}.qds.vw_rds_grid_1132 GRD 
	ON PR1.ID=GRD.PR_ID
Left join {0}.qds.vw_addtl_type   ADT_1134
	ON GRD.DF_1134=ADT_1134.ID
Left join {0}.qds.vw_addtl_type   ADT_1133
	ON GRD.DF_1133=ADT_1133.ID 
 where PR1.PROJECT_ID IN (16,17) """.format(Catalog_Name_L1,Audit_Job_ID)


# COMMAND ----------

############################################################################################################################
# Step 1: Enter in L2_Audit table for process start                                                                        #
# Step 2: Overwrite complaint_failure table                                                                                #
# Step 3a: Update Audit table entry  by setting as status Completed                                                        #
# Step 3b: In case of failure update  the Audit table entry  by updating the status as Error with an error message         #
############################################################################################################################
try: 
    Audit_Start_date_Time = datetime.now()
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,None ,Source_Name ,Table_Name ,'Inprogress',None, None,None, None)
    df_Complaint_Hazard=sqlContext.sql(Complaint_Hazard_Query)
    var_Complaint_Hazard_count = df_Complaint_Hazard.count()
    var_Complaint_ID_min=df_Complaint_Hazard.agg({'COMPLAINT_ID': 'min'}).collect()[0][0]
    var_Complaint_ID_max=df_Complaint_Hazard.agg({'COMPLAINT_ID': 'max'}).collect()[0][0]
    Delta_OverWrite(Table_Name,df_Complaint_Hazard )   
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Completed',var_Complaint_Hazard_count,var_Complaint_ID_max,var_Complaint_ID_min,None)
except Exception as err:
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Error', None,None, None,err)

# COMMAND ----------

# MAGIC %md #COMPLAINT_PATIENT_OUTCOME

# COMMAND ----------

################################################################################################################
# Find_Audit_Master_ID Function: Used to find Audit Master ID from Audit master table for Audit entry          #
################################################################################################################
Table_Name=Catalog_Name_L2+'.qnr.complaint_patient_outcome'
Audit_Table_Name='COMPLAINT_PATIENT_OUTCOME'
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)

# COMMAND ----------

Complaint_Patient_Outcome_Query="""
select cast(GRD.PR_ID as int) as COMPLAINT_ID ,
	17973 as APPLICATION_ID,
	GRD.SEQ_NO as SEQ_NO ,
	ADT_1126.NAME as ACTUAL_HARM_SEVERITY,
	'{1}' as Audit_Job_ID,
	GRD.ADLS_LOADED_DATE as L1_LOADED_DATE,
	GRD.LAST_UPDATED_DATE as L1_UPDATED_DATE,
	getdate() as  L2_LOADED_DATE,
	getdate() as L2_UPDATED_DATE
from {0}.qds.vw_rds_pr_1 PR1
Inner join {0}.qds.vw_rds_grid_1113 GRD 
	ON PR1.ID=GRD.PR_ID
Left join {0}.qds.vw_addtl_type   ADT_1126
	ON GRD.DF_1126=ADT_1126.ID
 where PR1.PROJECT_ID IN (16,17) """.format(Catalog_Name_L1,Audit_Job_ID)

# COMMAND ----------

############################################################################################################################
# Step 1: Enter in L2_Audit table for process start                                                                        #
# Step 2: Overwrite complaint_failure table                                                                                #
# Step 3a: Update Audit table entry  by setting as status Completed                                                        #
# Step 3b: In case of failure update  the Audit table entry  by updating the status as Error with an error message         #
############################################################################################################################
try: 
    Audit_Start_date_Time = datetime.now()
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,None ,Source_Name ,Table_Name ,'Inprogress',None, None,None, None)
    df_Complaint_Patient_Outcome=sqlContext.sql(Complaint_Patient_Outcome_Query)
    var_Complaint_Patient_Outcome_count = df_Complaint_Patient_Outcome.count()
    var_Complaint_ID_min=df_Complaint_Patient_Outcome.agg({'COMPLAINT_ID': 'min'}).collect()[0][0]
    var_Complaint_ID_max=df_Complaint_Patient_Outcome.agg({'COMPLAINT_ID': 'max'}).collect()[0][0]
    Delta_OverWrite(Table_Name,df_Complaint_Patient_Outcome)   
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Completed',var_Complaint_Patient_Outcome_count,var_Complaint_ID_max,var_Complaint_ID_min,None)
except Exception as err:
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Error', None,None, None,err)
   

# COMMAND ----------

# MAGIC %md #COMPLAINT_PART_USED
# MAGIC

# COMMAND ----------

################################################################################################################
# Find_Audit_Master_ID Function: Used to find Audit Master ID from Audit master table for Audit entry          #
################################################################################################################
Table_Name=Catalog_Name_L2+'.qnr.complaint_part_used'
Audit_Table_Name='COMPLAINT_PART_USED'
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)

# COMMAND ----------

Complaint_Part_Used_Query="""
select 
  GRD.PR_ID as COMPLAINT_ID ,
  17973 as APPLICATION_ID,
  GRD.SEQ_NO  as SEQ_NO,
  GRD.DF_1268 as CONSUMED_12NC,
  GRD.DF_1270 as CONSUMED_PART_DESCRIPTION,
  GRD.DF_1271 as CONSUMED_SERIAL_NUMBER,
  GRD.DF_237 as QUANTITY ,
  ADL_1117.NAME as PART_CLASSIFICATION,
  GRD.DF_1273 as DEFECTIVE_12NC,
  GRD.DF_1275 as DEFECTIVE_PART_DESCRIPTION,
  GRD.DF_1276 as DEFECTIVE_SERIAL_NUMBER,
  ADL_1118.NAME as DEFECTIVE_PART_FAILURE_REASON,
  ADL_1119.NAME as DEFECTIVE_PART_CATEGORY,
  '{1}' as Audit_Job_ID,
  GRD.ADLS_LOADED_DATE as L1_LOADED_DATE,
  GRD.LAST_UPDATED_DATE as L1_UPDATED_DATE,
  getdate() as  L2_LOADED_DATE,
  getdate() as L2_UPDATED_DATE
from {0}.qds.vw_rds_pr_1 as PR1
Inner join {0}.qds.vw_rds_grid_1266 as GRD 
  on GRD.PR_ID = PR1.ID   
Left join {0}.qds.vw_addtl_type ADL_1117
  on  ADL_1117.ID = GRD.DF_1117
Left join {0}.qds.vw_addtl_type ADL_1118
  on  ADL_1118.ID = GRD.DF_1118
Left join {0}.qds.vw_addtl_type ADL_1119
  on  ADL_1119.ID = GRD.DF_1119
Where PR1.PROJECT_ID IN (16,17)  """.format(Catalog_Name_L1,Audit_Job_ID)

# COMMAND ----------

############################################################################################################################
# Step 1: Enter in L2_Audit table for process start                                                                        #
# Step 2: Overwrite complaint_failure table                                                                                #
# Step 3a: Update Audit table entry  by setting as status Completed                                                        #
# Step 3b: In case of failure update  the Audit table entry  by updating the status as Error with an error message         #
############################################################################################################################
try: 
    Audit_Start_date_Time = datetime.now()
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,None ,Source_Name ,Table_Name ,'Inprogress',None, None,None, None)
    df_Complaint_Part_Used=sqlContext.sql(Complaint_Part_Used_Query)
    var_Complaint_Part_Used_count = df_Complaint_Part_Used.count()
    var_Complaint_ID_min=df_Complaint_Part_Used.agg({'COMPLAINT_ID': 'min'}).collect()[0][0]
    var_Complaint_ID_max=df_Complaint_Part_Used.agg({'COMPLAINT_ID': 'max'}).collect()[0][0]
    Delta_OverWrite(Table_Name,df_Complaint_Part_Used )   
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Completed',var_Complaint_Part_Used_count,var_Complaint_ID_max,var_Complaint_ID_min,None)
except Exception as err:
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Error', None,None, None,err)
    print(err)

# COMMAND ----------

# MAGIC %md #COMPLAINT_DUPLICATE

# COMMAND ----------

################################################################################################################
# Find_Audit_Master_ID Function: Used to find Audit Master ID from Audit master table for Audit entry          #
################################################################################################################
Table_Name=Catalog_Name_L2+'.qnr.complaint_duplicate'
Audit_Table_Name='COMPLAINT_DUPLICATE'
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)

# COMMAND ----------

Complaint_Duplicate_Query="""
select GRD.PR_ID as COMPLAINT_ID ,
		17973 as APPLICATION_ID,
		DF_510  as COMPLAINT_ID_DUPLICATE, 
		17973 as APPLICATION_ID_DUPLICATE,
		'{1}' as Audit_Job_ID,
		GRD.ADLS_LOADED_DATE as L1_LOADED_DATE,
		GRD.LAST_UPDATED_DATE as L1_UPDATED_DATE,
		getdate() as  L2_LOADED_DATE,
		getdate() as L2_UPDATED_DATE
from {0}.qds.vw_rds_pr_1 as PR1
Inner join {0}.qds.vw_rds_ref_record_510 as GRD 
	on GRD.PR_ID = PR1.ID 
Where PR1.PROJECT_ID IN (16,17) """.format(Catalog_Name_L1,Audit_Job_ID)


# COMMAND ----------

############################################################################################################################
# Step 1: Enter in L2_Audit table for process start                                                                        #
# Step 2: Overwrite complaint_failure table                                                                                #
# Step 3a: Update Audit table entry  by setting as status Completed                                                        #
# Step 3b: In case of failure update  the Audit table entry  by updating the status as Error with an error message         #
############################################################################################################################
try: 
    Audit_Start_date_Time = datetime.now()
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,None ,Source_Name ,Table_Name ,'Inprogress',None, None,None, None)
    df_Complaint_Duplicate=sqlContext.sql(Complaint_Duplicate_Query)
    var_Complaint_Duplicate_Count = df_Complaint_Duplicate.count()
    var_Complaint_ID_min=df_Complaint_Duplicate.agg({'COMPLAINT_ID': 'min'}).collect()[0][0]
    var_Complaint_ID_max=df_Complaint_Duplicate.agg({'COMPLAINT_ID': 'max'}).collect()[0][0]
    Delta_OverWrite(Table_Name,df_Complaint_Duplicate )   
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Completed',var_Complaint_Duplicate_Count,var_Complaint_ID_max,var_Complaint_ID_min,None)
except Exception as err:
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Error', None,None, None,err)

# COMMAND ----------

# MAGIC %md #COMPLAINT_EVALUATION_RESULT

# COMMAND ----------

################################################################################################################
# Find_Audit_Master_ID Function: Used to find Audit Master ID from Audit master table for Audit entry          #
################################################################################################################
Table_Name=Catalog_Name_L2+'.qnr.complaint_evaluation_result'
Audit_Table_Name='COMPLAINT_EVALUATION_RESULT'
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)

# COMMAND ----------

Complaint_Evaluation_Result_Query="""
select GRD.PR_ID as COMPLAINT_ID ,
	17973 as APPLICATION_ID,
	GRD.SEQ_NO  as SEQ_NO,
	ADL1.NAME as EVALUATION_RESULT_CODE_L1,
	ADL2.NAME as EVALUATION_RESULT_CODE_L2,
	ADL3.NAME as EVALUATION_RESULT_CODE_L3,
	'{1}' as Audit_Job_ID,
	GRD.ADLS_LOADED_DATE as L1_LOADED_DATE,
	GRD.LAST_UPDATED_DATE as L1_UPDATED_DATE,
	getdate() as  L2_LOADED_DATE,
	getdate() as L2_UPDATED_DATE
from {0}.qds.vw_rds_pr_1 as PR1
Inner join {0}.qds.vw_rds_grid_1326 as GRD 
  on GRD.PR_ID = PR1.ID    
Left join {0}.qds.vw_addtl_type ADL1
  on  ADL1.ID = GRD.DF_1327
Left join {0}.qds.vw_addtl_type ADL2
  on  ADL2.ID = GRD.DF_1328
Left join {0}.qds.vw_addtl_type ADL3 
  on  ADL3.ID = GRD.DF_1329
Where PR1.PROJECT_ID IN (16,17)  """.format(Catalog_Name_L1,Audit_Job_ID)

# COMMAND ----------

############################################################################################################################
# Step 1: Enter in L2_Audit table for process start                                                                        #
# Step 2: Overwrite complaint_failure table                                                                                #
# Step 3a: Update Audit table entry  by setting as status Completed                                                        #
# Step 3b: In case of failure update  the Audit table entry  by updating the status as Error with an error message         #
############################################################################################################################
try: 
    Audit_Start_date_Time = datetime.now()
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,None ,Source_Name ,Table_Name ,'Inprogress',None, None,None, None)
    df_Complaint_Evaluation_Result=sqlContext.sql(Complaint_Evaluation_Result_Query)
    var_Complaint_Evaluation_Result_Count = df_Complaint_Evaluation_Result.count()
    var_Complaint_ID_min=df_Complaint_Evaluation_Result.agg({'COMPLAINT_ID': 'min'}).collect()[0][0]
    var_Complaint_ID_max=df_Complaint_Evaluation_Result.agg({'COMPLAINT_ID': 'max'}).collect()[0][0]
    Delta_OverWrite(Table_Name,df_Complaint_Evaluation_Result )   
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Completed',var_Complaint_Evaluation_Result_Count,var_Complaint_ID_max,var_Complaint_ID_min,None)
except Exception as err:
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Error', None,None, None,err)

# COMMAND ----------

# MAGIC %md #COMPLAINT_COMPONENT

# COMMAND ----------

################################################################################################################
# Find_Audit_Master_ID Function: Used to find Audit Master ID from Audit master table for Audit entry          #
################################################################################################################
Table_Name=Catalog_Name_L2+'.qnr.complaint_component'
Audit_Table_Name='COMPLAINT_COMPONENT'
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)

# COMMAND ----------

Complaint_Component_Query="""
select GRD.PR_ID as COMPLAINT_ID ,
    17973 as APPLICATION_ID,
    GRD.SEQ_NO as SEQ_NO,
    ADL1.NAME as COMPONENT_CODE_L1,
    ADL2.NAME as COMPONENT_CODE_L2,
    ADL3.NAME as COMPONENT_CODE_L3,
	'{1}' as Audit_Job_ID,
	GRD.ADLS_LOADED_DATE as L1_LOADED_DATE,
	GRD.LAST_UPDATED_DATE as L1_UPDATED_DATE,
	getdate() as  L2_LOADED_DATE,
	getdate() as L2_UPDATED_DATE
from  {0}.qds.vw_rds_pr_1 as PR1
Inner join {0}.qds.vw_rds_grid_1332 as GRD 
  on GRD.PR_ID = PR1.ID   
Left join {0}.qds.vw_addtl_type ADL1
  on  ADL1.ID = GRD.DF_1333
Left join {0}.qds.vw_addtl_type ADL2
  on  ADL2.ID = GRD.DF_1334
Left join {0}.qds.vw_addtl_type ADL3 
  on  ADL3.ID = GRD.DF_1335 
  where PR1.PROJECT_ID IN (16,17) """.format(Catalog_Name_L1,Audit_Job_ID )

# COMMAND ----------

############################################################################################################################
# Step 1: Enter in L2_Audit table for process start                                                                        #
# Step 2: Overwrite complaint_failure table                                                                                #
# Step 3a: Update Audit table entry  by setting as status Completed                                                        #
# Step 3b: In case of failure update  the Audit table entry  by updating the status as Error with an error message         #
############################################################################################################################
try: 
    Audit_Start_date_Time = datetime.now()
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,None ,Source_Name ,Table_Name ,'Inprogress',None, None,None, None)
    df_Complaint_Component_Query=sqlContext.sql(Complaint_Component_Query)
    var_Complaint_Component_Count = df_Complaint_Component_Query.count()
    var_Complaint_ID_min=df_Complaint_Component_Query.agg({'COMPLAINT_ID': 'min'}).collect()[0][0]
    var_Complaint_ID_max=df_Complaint_Component_Query.agg({'COMPLAINT_ID': 'max'}).collect()[0][0]
    Delta_OverWrite(Table_Name,df_Complaint_Component_Query )   
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Completed',var_Complaint_Component_Count,var_Complaint_ID_max,var_Complaint_ID_min,None)
except Exception as err:
    Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Error', None,None, None,err)
