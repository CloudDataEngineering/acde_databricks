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
from pyspark.sql.functions import concat_ws,col

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

dbutils.widgets.text("load_type", "", "load_type")
load_type  = dbutils.widgets.get("load_type")



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
Source_Name='CatsWeb'

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

if load_type == "FULL":
    Query_complaint = """ select * from (
    select 
    c.COMPLAINT_ID	as	COMPLAINT_ID,
    '24500'	as	APPLICATION_ID,
    c.STATE	as	COMPLAINT_STATE,
    cast(c.EVENT_DATE as date)	as	EVENT_DATE,
    c.CREATED_DATE	as	DATE_OPENED,
    c.DEVICE_TYPE	as	PRODUCT_FAMILY,
    c.DIVISION	as	OWNING_ENTITY_NAME,
    c.PRODUCT_MODEL_NR	as	CATALOG_ITEM_ID,
    c.PRODUCT_NAME	as	CATALOG_ITEM_NAME,
    NULL as	TRADE_ITEM_ID,
    NULL as	TRADE_ITEM_NAME,
    NULL as	DEVICE_IDENTIFIER,
    NULL as	IS_THIRD_PARTY_DEVICE,
    NULL as	MODEL_NUMBER,
    NULL as	IS_MEDICAL_DEVICE,
    c.UDI	as	PRODUCT_UDI,
    c.MANUFACTURED_DATE	as	MANUFACTURE_DATE,
    NULL as	SHIP_DATE,
    c.EXPIRATION_DATE	as	EXPIRATION_DATE,
    NULL as	REPORTER_COUNTRY,
    c.COUNTRY	as	EVENT_COUNTRY,
    NULL as	PHILIPS_NOTIFIED_DATE,
    NULL as	INITIATE_DATE,
    TO_TIMESTAMP(c.COMPLAINT_AWARE_DATE, 'M/d/yyyy h:mm:ss a')	as	BECOME_AWARE_DATE,
    NULL as	COMPLAINT_SHORT_DESCRIPTION,
    c.NATURE_OF_COMPLAINT	as	COMPLAINT_CUSTOMER_DESCRIPTION,
    c.REPORTED_PROBLEM_L1	as	REPORTED_PROBLEM_CODE_L1,
    c.REPORTED_PROBLEM_L2	as	REPORTED_PROBLEM_CODE_L2,
    c.PROBLEM_CODE	as	REPORTED_PROBLEM_CODE_L3,
    c.WHEN_DID_PROBLEM_OCCUR	as	DEVICE_USE_AT_TIME_OF_EVENT,
    NULL as	IS_PATIENT_USER_HARMED,
    case when  lower(c.POTENTIAL_SAFETY_REVIEW) ='no' then 'N' when  lower(c.POTENTIAL_SAFETY_REVIEW) ='yes' then 'Y' else Null end  	as	IS_POTENTIAL_SAFETY_ALERT,
    NULL as	IS_POTENTIAL_SAFETY_EVENT,
    case when  lower(c.DID_DEATH_OR_SI_OCCUR) ='no' then 'N' when  lower(c.DID_DEATH_OR_SI_OCCUR )='yes' then 'Y' else Null end  	as	IS_ALLEGATION_OF_INJURY_OR_DEATH,
    NULL as	HAS_DEVICE_ALARMED,
    NULL as	INCIDENT_KEY_WORDS,
    c.TYPE_OF_REPORTABLE_EVENT	as	TYPE_OF_REPORTED_COMPLAINT,
    NULL as	COMPLAINT_LONG_DESCRIPTION,
    NULL as	SOURCE_NOTES,
    c.INVESTIGATION_SUMMARY	as	INVESTIGATION_SUMMARY,
    NULL as	INVESTIGATION_NOTES,
    c.PROBABLE_CAUSE_CODE	as	PROBLEM_SOURCE_CODE,
    c.AS_ANALYZED_CODE	as	PROBLEM_REASON_CODE,
    case when  lower(c.IS_CAPA_REQUIRED) ='no' then 'N' when  lower(c.IS_CAPA_REQUIRED) ='yes' then 'Y' else Null end  	as	IS_CAPA_ADDTL_INVEST_REQUIRED,
    NULL as	OTHER_RELEVANT_EVENT_INFO,
    NULL as	PATIENT_ACTIONS_TAKEN,
    NULL as	PROBLEM_SYMPTOMS_AND_FREQUENCY,
    NULL as	ADDTL_INFORMATION,
    NULL as SOURCE_SYSTEM,
    c.COMPLAINT_SOURCE	as	SOURCE_OF_COMPLAINT,
    NULL as SOURCE_SERIAL_NUMBER,
    NULL as SERIAL_NUMBER, 
    NULL as DATE_CREATED,
    NULL as  DATE_CLOSED,
    NULL as  SOURCE_CATALOG_ITEM_ID,
    NULL as SOURCE_CATALOG_ITEM_NAME,
    NULL as LOT_OR_BATCH_NUMBER,
    NULL as SOLUTION_FOR_THE_CUSTOMER,
    NULL as  CUSTOMER_NUMBER,
    NULL as NO_FURTHER_INVEST_REQ_ON,
    NULL as SOURCE_EVENT_COUNTRY,
    NULL as ADDITIONAL_EVALUATION_COMMENTS,
    NULL as SYMPTOM_CODE_1,
    NULL as SYMPTOM_CODE_2,
    NULL as SYMPTOM_CODE_3,
    '{1}' as Audit_Job_ID,
    ADLS_LOADED_DATE as L1_LOADED_DATE,
    LAST_UPDATED_DATE as L1_UPDATED_DATE,
    getdate() as  L2_LOADED_DATE,
    getdate() as L2_UPDATED_DATE,
    row_number() over ( partition by COMPLAINT_ID  order by LAST_UPDATED_DATE) as rk
    from {0}.catsweb.complaint as c ) sl where rk = 1 """.format(Catalog_Name_L1,Audit_Job_ID,Water_Mark_DT)
    # print(Query_complaint)
    df_complaint= sqlContext.sql(Query_complaint)
    #display(df_complaint.filter(df_complaint["COMPLAINT_ID"].isin([342,332,343,346])))
    #display(df_complaint)


# COMMAND ----------

# DBTITLE 1,Create Source Query Data frame
if load_type != "FULL":
    Query_complaint = """
    select 
    c.COMPLAINT_ID	as	COMPLAINT_ID,
    '24500'	as	APPLICATION_ID,
    c.STATE	as	COMPLAINT_STATE,
    cast(c.EVENT_DATE as date)	as	EVENT_DATE,
    c.CREATED_DATE	as	DATE_OPENED,
    c.DEVICE_TYPE	as	PRODUCT_FAMILY,
    c.DIVISION	as	OWNING_ENTITY_NAME,
    c.PRODUCT_MODEL_NR	as	CATALOG_ITEM_ID,
    c.PRODUCT_NAME	as	CATALOG_ITEM_NAME,
    NULL as	TRADE_ITEM_ID,
    NULL as	TRADE_ITEM_NAME,
    NULL as	DEVICE_IDENTIFIER,
    NULL as	IS_THIRD_PARTY_DEVICE,
    NULL as	MODEL_NUMBER,
    NULL as	IS_MEDICAL_DEVICE,
    c.UDI	as	PRODUCT_UDI,
    c.MANUFACTURED_DATE	as	MANUFACTURE_DATE,
    NULL as	SHIP_DATE,
    c.EXPIRATION_DATE	as	EXPIRATION_DATE,
    NULL as	REPORTER_COUNTRY,
    c.COUNTRY	as	EVENT_COUNTRY,
    NULL as	PHILIPS_NOTIFIED_DATE,
    NULL as	INITIATE_DATE,
    TO_TIMESTAMP(c.COMPLAINT_AWARE_DATE, 'M/d/yyyy h:mm:ss a')	as	BECOME_AWARE_DATE,
    NULL as	COMPLAINT_SHORT_DESCRIPTION,
    c.NATURE_OF_COMPLAINT	as	COMPLAINT_CUSTOMER_DESCRIPTION,
    c.REPORTED_PROBLEM_L1	as	REPORTED_PROBLEM_CODE_L1,
    c.REPORTED_PROBLEM_L2	as	REPORTED_PROBLEM_CODE_L2,
    c.PROBLEM_CODE	as	REPORTED_PROBLEM_CODE_L3,
    c.WHEN_DID_PROBLEM_OCCUR	as	DEVICE_USE_AT_TIME_OF_EVENT,
    NULL as	IS_PATIENT_USER_HARMED,
    case when  lower(c.POTENTIAL_SAFETY_REVIEW) ='no' then 'N' when  lower(c.POTENTIAL_SAFETY_REVIEW) ='yes' then 'Y' else Null end  	as	IS_POTENTIAL_SAFETY_ALERT,
    NULL as	IS_POTENTIAL_SAFETY_EVENT,
    case when  lower(c.DID_DEATH_OR_SI_OCCUR) ='no' then 'N' when  lower(c.DID_DEATH_OR_SI_OCCUR )='yes' then 'Y' else Null end  	as	IS_ALLEGATION_OF_INJURY_OR_DEATH,
    NULL as	HAS_DEVICE_ALARMED,
    NULL as	INCIDENT_KEY_WORDS,
    c.TYPE_OF_REPORTABLE_EVENT	as	TYPE_OF_REPORTED_COMPLAINT,
    NULL as	COMPLAINT_LONG_DESCRIPTION,
    NULL as	SOURCE_NOTES,
    c.INVESTIGATION_SUMMARY	as	INVESTIGATION_SUMMARY,
    NULL as	INVESTIGATION_NOTES,
    c.PROBABLE_CAUSE_CODE	as	PROBLEM_SOURCE_CODE,
    c.AS_ANALYZED_CODE	as	PROBLEM_REASON_CODE,
    case when  lower(c.IS_CAPA_REQUIRED) ='no' then 'N' when  lower(c.IS_CAPA_REQUIRED) ='yes' then 'Y' else Null end  	as	IS_CAPA_ADDTL_INVEST_REQUIRED,
    NULL as	OTHER_RELEVANT_EVENT_INFO,
    NULL as	PATIENT_ACTIONS_TAKEN,
    NULL as	PROBLEM_SYMPTOMS_AND_FREQUENCY,
    NULL as	ADDTL_INFORMATION,
    NULL as SOURCE_SYSTEM,
    c.COMPLAINT_SOURCE	as	SOURCE_OF_COMPLAINT,
    NULL as SOURCE_SERIAL_NUMBER,
    NULL as SERIAL_NUMBER, 
    NULL as DATE_CREATED,
    NULL as  DATE_CLOSED,
    NULL as  SOURCE_CATALOG_ITEM_ID,
    NULL as SOURCE_CATALOG_ITEM_NAME,
    NULL as LOT_OR_BATCH_NUMBER,
    NULL as SOLUTION_FOR_THE_CUSTOMER,
    NULL as  CUSTOMER_NUMBER,
    NULL as NO_FURTHER_INVEST_REQ_ON,
    NULL as SOURCE_EVENT_COUNTRY,
    NULL as ADDITIONAL_EVALUATION_COMMENTS,
    NULL as SYMPTOM_CODE_1,
    NULL as SYMPTOM_CODE_2,
    NULL as SYMPTOM_CODE_3,
    '{1}' as Audit_Job_ID,
    ADLS_LOADED_DATE as L1_LOADED_DATE,
    LAST_UPDATED_DATE as L1_UPDATED_DATE,
    getdate() as  L2_LOADED_DATE,
    getdate() as L2_UPDATED_DATE
    from {0}.catsweb.complaint as c
    where LAST_UPDATED_DATE >= '{2}'  
    or COMPLAINT_ID  in (select distinct  COMPLAINT_ID from {0}.catsweb.complaint_comments_decon 
where COMMENTS_DECON is not null and (LAST_EDIT_DATE >= '{2}'  or LAST_CREATED_DATE >= '{2}' ))
or COMPLAINT_ID  in  (select distinct  COMPLAINT_ID 
from {0}.catsweb.complaint_comments_field_service 
where COMMENTS_FIELD_SERVICE is not null and (LAST_EDIT_DATE >= '{2}'  or LAST_CREATED_DATE >= '{2}' ))
or COMPLAINT_ID  in  (select distinct COMPLAINT_ID  
from {0}.catsweb.complaint_notes 
where COMPLAINT_NOTES is not null and ( LAST_EDIT_DATE >= '{2}'  or LAST_CREATED_DATE >= '{2}' ))
or COMPLAINT_ID  in (select distinct COMPLAINT_ID 
from {0}.catsweb.complaint_hazard 
where  LAST_EDIT_DATE >= '{2}'  or LAST_CREATED_DATE >= '{2}')
or COMPLAINT_ID  in (  select distinct COMPLAINT_ID  
from {0}.catsweb.complaint_usca_dt_notes 
where USCA_DT_NOTES is not null and (LAST_EDIT_DATE >= '{2}'  or LAST_CREATED_DATE >= '{2}' ))
 """.format(Catalog_Name_L1,Audit_Job_ID,Water_Mark_DT)
    # print(Query_complaint)
    df_complaint= sqlContext.sql(Query_complaint)
    #display(df_complaint.filter(col("complaint_id") =='217944'))

# COMMAND ----------

Query_complaint_comments_decon = """select COMPLAINT_ID as COMPLAINT_ID_D,
CONCAT_WS('\n', COLLECT_LIST(COMMENTS_DECON))  as COMMENTS_DECON 
from ( select COMPLAINT_ID ,COMMENTS_DECON
from {0}.catsweb.complaint_comments_decon 
where COMMENTS_DECON is not null
ORDER BY COMPLAINT_ID, LAST_EDIT_DATE asc, LAST_CREATED_DATE asc ) GROUP BY COMPLAINT_ID """.format(Catalog_Name_L1)
df_complaint_comments_decon= sqlContext.sql(Query_complaint_comments_decon)

Query_complaint_comments_field_service  = """select COMPLAINT_ID as COMPLAINT_ID_FS ,
CONCAT_WS('\n', COLLECT_LIST(COMMENTS_FIELD_SERVICE))  as COMMENTS_FIELD_SERVICE
 from ( select COMPLAINT_ID , COMMENTS_FIELD_SERVICE
from {0}.catsweb.complaint_comments_field_service 
where COMMENTS_FIELD_SERVICE is not null
ORDER BY COMPLAINT_ID, LAST_EDIT_DATE asc, LAST_CREATED_DATE asc ) GROUP BY COMPLAINT_ID""".format(Catalog_Name_L1)
df_complaint_comments_field_service= sqlContext.sql(Query_complaint_comments_field_service)

Query_complaint_notes = """select COMPLAINT_ID as COMPLAINT_ID_N,CONCAT_WS('\n', COLLECT_LIST(COMPLAINT_NOTES))   as COMPLAINT_NOTES
from ( 
select COMPLAINT_ID ,COMPLAINT_NOTES 
from {0}.catsweb.complaint_notes 
where COMPLAINT_NOTES is not null
ORDER BY COMPLAINT_ID, LAST_EDIT_DATE asc, LAST_CREATED_DATE asc ) GROUP BY COMPLAINT_ID """.format(Catalog_Name_L1)
df_complaint_notes= sqlContext.sql(Query_complaint_notes)

# Query_complaint_hazard = """select COMPLAINT_ID as COMPLAINT_ID_H,CONCAT_WS(' ', COLLECT_LIST(HAZARDOUS_SITUATION))   as HAZARDOUS_SITUATION from ( select COMPLAINT_ID, HAZARDOUS_SITUATION
# from dev_l1.catsweb.complaint_hazard 
# where HAZARDOUS_SITUATION is not null
# ORDER BY COMPLAINT_ID, LAST_EDIT_DATE asc, LAST_CREATED_DATE asc ) GROUP BY COMPLAINT_ID  """
# df_complaint_hazard= sqlContext.sql(Query_complaint_hazard)
# #df_complaint_hazard.show()

Query_complaint_hazard = """select COMPLAINT_ID as COMPLAINT_ID_H,HAZARDOUS_SITUATION from ( select * , row_number() OVER(partition by COMPLAINT_ID ORDER BY LAST_EDIT_DATE desc ,LAST_CREATED_DATE desc ) as RowNumber_ccd 
from {0}.catsweb.complaint_hazard ) where RowNumber_ccd = 1 """.format(Catalog_Name_L1)
df_complaint_hazard= sqlContext.sql(Query_complaint_hazard)
#df_complaint_hazard.show()

Query_complaint_usca_dt_notes  = """select COMPLAINT_ID as COMPLAINT_ID_UDN,CONCAT_WS('\n', COLLECT_LIST(USCA_DT_NOTES))  as USCA_DT_NOTES 
from ( select COMPLAINT_ID ,USCA_DT_NOTES 
from {0}.catsweb.complaint_usca_dt_notes 
where USCA_DT_NOTES is not null
ORDER BY COMPLAINT_ID, LAST_EDIT_DATE asc, LAST_CREATED_DATE asc ) GROUP BY COMPLAINT_ID """.format(Catalog_Name_L1)
df_complaint_usca_dt_notes = sqlContext.sql(Query_complaint_usca_dt_notes )



# COMMAND ----------

df_complaint_2 = df_complaint.join(df_complaint_comments_decon , df_complaint.COMPLAINT_ID == df_complaint_comments_decon.COMPLAINT_ID_D, "Left")

df_complaint_3 = df_complaint_2.join(df_complaint_comments_field_service , df_complaint.COMPLAINT_ID == df_complaint_comments_field_service.COMPLAINT_ID_FS, "Left")

df_complaint_4 = df_complaint_3.join(df_complaint_notes , df_complaint.COMPLAINT_ID == df_complaint_notes.COMPLAINT_ID_N, "Left")

df_complaint_5 = df_complaint_4.join(df_complaint_hazard , df_complaint.COMPLAINT_ID == df_complaint_hazard.COMPLAINT_ID_H, "Left")


df_complaint_6 = df_complaint_5.join(df_complaint_usca_dt_notes , df_complaint.COMPLAINT_ID ==  df_complaint_usca_dt_notes.COMPLAINT_ID_UDN, "Left")

#df_complaint_6.filter(col("COMPLAINT_ID_UDN") =='217944').show()

# COMMAND ----------

#.filter("COMPLAINT_ID=1086 and APPLICATION_ID= 24500")
df_complaint_final= df_complaint_6.select("COMPLAINT_ID","APPLICATION_ID","COMPLAINT_STATE","DATE_OPENED",
"EVENT_DATE","PRODUCT_FAMILY","OWNING_ENTITY_NAME",
"CATALOG_ITEM_ID","CATALOG_ITEM_NAME","TRADE_ITEM_ID",
"TRADE_ITEM_NAME","DEVICE_IDENTIFIER","IS_THIRD_PARTY_DEVICE",
"MODEL_NUMBER","IS_MEDICAL_DEVICE","PRODUCT_UDI",
"MANUFACTURE_DATE","SHIP_DATE","EXPIRATION_DATE",
"REPORTER_COUNTRY","EVENT_COUNTRY","PHILIPS_NOTIFIED_DATE",
"INITIATE_DATE","BECOME_AWARE_DATE","COMPLAINT_SHORT_DESCRIPTION",
"COMPLAINT_CUSTOMER_DESCRIPTION","REPORTED_PROBLEM_CODE_L1","REPORTED_PROBLEM_CODE_L2",
"REPORTED_PROBLEM_CODE_L3","IS_POTENTIAL_SAFETY_ALERT","IS_POTENTIAL_SAFETY_EVENT",
"DEVICE_USE_AT_TIME_OF_EVENT","IS_PATIENT_USER_HARMED","IS_POTENTIAL_SAFETY_ALERT","IS_POTENTIAL_SAFETY_EVENT","IS_ALLEGATION_OF_INJURY_OR_DEATH",
"HAS_DEVICE_ALARMED","INCIDENT_KEY_WORDS","TYPE_OF_REPORTED_COMPLAINT",
"HAZARDOUS_SITUATION","COMPLAINT_LONG_DESCRIPTION","SOURCE_NOTES","COMPLAINT_NOTES","COMMENTS_DECON","COMMENTS_FIELD_SERVICE",
concat(lit("COMPLAINT_NOTES:"),lit('\n'),coalesce("COMPLAINT_NOTES",lit(" ")),lit('\n'),lit("COMMENTS_DECON:"),lit('\n'),coalesce("COMMENTS_DECON",lit(" ")),lit('\n'),lit("COMMENTS_FIELD_SERVICE:"),lit('\n'),coalesce("COMMENTS_FIELD_SERVICE",lit(" "))).alias("COMMENTS"),
#concat(coalesce("COMPLAINT_NOTES",lit(" ")),coalesce("COMMENTS_DECON",lit(" ")),coalesce("COMMENTS_FIELD_SERVICE",lit(" "))).alias("COMMENTS"),
"INVESTIGATION_SUMMARY","INVESTIGATION_NOTES","PROBLEM_SOURCE_CODE","PROBLEM_REASON_CODE",
"IS_CAPA_ADDTL_INVEST_REQUIRED","OTHER_RELEVANT_EVENT_INFO","PATIENT_ACTIONS_TAKEN",
"PROBLEM_SYMPTOMS_AND_FREQUENCY","ADDTL_INFORMATION",col("USCA_DT_NOTES").alias("MEDICAL_RATIONALE"),
"SOURCE_SYSTEM",
"SOURCE_OF_COMPLAINT",
"SOURCE_SERIAL_NUMBER",
"SERIAL_NUMBER",
"DATE_CREATED",
"DATE_CLOSED",
"SOURCE_CATALOG_ITEM_ID",
"SOURCE_CATALOG_ITEM_NAME",
"LOT_OR_BATCH_NUMBER",
"SOLUTION_FOR_THE_CUSTOMER",
"CUSTOMER_NUMBER",
"NO_FURTHER_INVEST_REQ_ON",
"SOURCE_EVENT_COUNTRY",
"ADDITIONAL_EVALUATION_COMMENTS",
"SYMPTOM_CODE_1",
"SYMPTOM_CODE_2",
"SYMPTOM_CODE_3","Audit_Job_ID",
"L1_LOADED_DATE","L1_UPDATED_DATE","L2_LOADED_DATE","L2_UPDATED_DATE")
df_complaint_final.persist()
#display(df_complaint_final.select("COMPLAINT_NOTES","COMMENTS_DECON","COMMENTS_FIELD_SERVICE",'COMMENTS'))

# COMMAND ----------

# DBTITLE 1,Scrubbing
email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
name_regex = r"(Prof.\sdr.\s\w+|Prof.dr.\s\w+|Prof.dr.\w+|Prof.\sdr.\w+|Prof.\sDr.\s\w+|Prof.Dr.\s\w+|Prof.Dr.\w+|Prof.\sDr.\w+|Dr\.\s\w+|Mr\.\s\w+|Dr\.\w+|Mr\.\w+|Prof\.\w+|Prof\.\s\w+|Ms\.\w+|Ms\.\s\w+|Mrs\.\w+|Mrs\.\s\w+|Miss\s\w+|Herr\s\w+|Fräulein\s\w+|Frau\s\w+|Mr./Mrs.\s\w+|Mr./Mrs.\w+|Mr./Ms.\w+|Mr./Ms.\s\w+)"
#phone_regex = r"(\+\d{4,18})"
phone_regex =r"(\+\d{6,18})|(\+\d{1,3}\s\d{3}[-]\d{3}[-]\d{3,4})|(\+\d{1,3}\s[(]\d{3}[)]\s\d{3}[-]\d{3,4})|(\+\d{1,3}[-]\d{6,9})|(\+\d{1,3}[-]\d{2,5}\s\d{3,5})|(\+\d{4}[-]\d{3}[-]\d{4})|(\+\d{1,3}[-]\d{2,3}[-]\d{2,3}[-]\d{2,5})|(\+\d{3}[-]\d{3}[-]\d{4})|(\+\d{3}[-]\d{1}[-]\d{4}[-]\d{3})|(\+\d{3}[-]\d{2}[-]\d{7})|(\+\d{3}[-]\d{3}[-]\d{5})|(\+\d{1,3})"

df_Complaint_scrubbed= df_complaint_final.select("COMPLAINT_ID","APPLICATION_ID","COMPLAINT_STATE","EVENT_DATE","DATE_OPENED",
"PRODUCT_FAMILY","OWNING_ENTITY_NAME",
"CATALOG_ITEM_ID","CATALOG_ITEM_NAME","TRADE_ITEM_ID",
"TRADE_ITEM_NAME","DEVICE_IDENTIFIER","IS_THIRD_PARTY_DEVICE",
"MODEL_NUMBER","IS_MEDICAL_DEVICE","PRODUCT_UDI",
"MANUFACTURE_DATE","SHIP_DATE","EXPIRATION_DATE",
"REPORTER_COUNTRY","EVENT_COUNTRY","PHILIPS_NOTIFIED_DATE",
"INITIATE_DATE","BECOME_AWARE_DATE","COMPLAINT_SHORT_DESCRIPTION",
regexp_replace(regexp_replace(regexp_replace("COMPLAINT_CUSTOMER_DESCRIPTION", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("COMPLAINT_CUSTOMER_DESCRIPTION")
,"REPORTED_PROBLEM_CODE_L1","REPORTED_PROBLEM_CODE_L2",
"REPORTED_PROBLEM_CODE_L3",
"DEVICE_USE_AT_TIME_OF_EVENT","IS_PATIENT_USER_HARMED","IS_POTENTIAL_SAFETY_ALERT","IS_POTENTIAL_SAFETY_EVENT","IS_ALLEGATION_OF_INJURY_OR_DEATH",
"HAS_DEVICE_ALARMED","INCIDENT_KEY_WORDS","TYPE_OF_REPORTED_COMPLAINT",
"HAZARDOUS_SITUATION","COMPLAINT_LONG_DESCRIPTION","SOURCE_NOTES",
regexp_replace(regexp_replace(regexp_replace("COMMENTS", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("COMMENTS"),
regexp_replace(regexp_replace(regexp_replace("INVESTIGATION_SUMMARY", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("INVESTIGATION_SUMMARY")
,regexp_replace(regexp_replace(regexp_replace("INVESTIGATION_NOTES", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("INVESTIGATION_NOTES"),"PROBLEM_SOURCE_CODE","PROBLEM_REASON_CODE",
"IS_CAPA_ADDTL_INVEST_REQUIRED","OTHER_RELEVANT_EVENT_INFO","PATIENT_ACTIONS_TAKEN",
"PROBLEM_SYMPTOMS_AND_FREQUENCY","ADDTL_INFORMATION"
,regexp_replace(regexp_replace(regexp_replace("MEDICAL_RATIONALE", phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("MEDICAL_RATIONALE"),
"SOURCE_SYSTEM",
"SOURCE_OF_COMPLAINT",
"SOURCE_SERIAL_NUMBER",
"SERIAL_NUMBER",
"DATE_CREATED",
"DATE_CLOSED",
"SOURCE_CATALOG_ITEM_ID",
"SOURCE_CATALOG_ITEM_NAME",
"LOT_OR_BATCH_NUMBER",
"SOLUTION_FOR_THE_CUSTOMER",
"CUSTOMER_NUMBER",
"NO_FURTHER_INVEST_REQ_ON",
"SOURCE_EVENT_COUNTRY",
"ADDITIONAL_EVALUATION_COMMENTS",
"SYMPTOM_CODE_1",
"SYMPTOM_CODE_2",
"SYMPTOM_CODE_3","Audit_Job_ID"
,"L1_LOADED_DATE","L1_UPDATED_DATE","L2_LOADED_DATE","L2_UPDATED_DATE")
df_Complaint_scrubbed.persist()
df_complaint_final.unpersist()
#df_Complaint_scrubbed.show()
df_Complaint_scrubbed.unpersist()

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

# MAGIC %md #COMPLAINT_HAZARD Code

# COMMAND ----------

################################################################################################################
# Find_Audit_Master_ID Function: Used to find Audit Master ID from Audit master table for Audit entry          #
################################################################################################################
Table_Name=Catalog_Name_L2+'.qnr.complaint_hazard_catsweb'
Audit_Table_Name='COMPLAINT_HAZARD_CATSWEB'
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)

# COMMAND ----------

Complaint_Hazard_Query="""
select 
cast(COMPLAINT_ID as bigint) as COMPLAINT_ID ,
24500 as APPLICATION_ID,
row_number() OVER(partition by COMPLAINT_ID ORDER BY  LAST_CREATED_DATE asc)  as SEQ_NO,
HAZARD_CATEGORY,
HAZARD,
'{1}' as Audit_Job_ID,
ADLS_LOADED_DATE as L1_LOADED_DATE,
LAST_UPDATED_DATE as L1_UPDATED_DATE,
getdate() as  L2_LOADED_DATE,
getdate() as L2_UPDATED_DATE
from {0}.catsweb.complaint_hazard """.format(Catalog_Name_L1,Audit_Job_ID)


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
Table_Name=Catalog_Name_L2+'.qnr.complaint_patient_outcome_catsweb'
Audit_Table_Name='COMPLAINT_PATIENT_OUTCOME_CATSWEB'
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)

# COMMAND ----------

Complaint_Patient_Outcome_Query="""
select 
  COMPLAINT_ID ,
  24500 as APPLICATION_ID,
  row_number() OVER(partition by COMPLAINT_ID ORDER BY  LAST_CREATED_DATE asc)  as SEQ_NO,
  SEVERITY as ACTUAL_HARM_SEVERITY ,
  '{1}' as Audit_Job_ID,
  ADLS_LOADED_DATE as L1_LOADED_DATE,
  LAST_UPDATED_DATE as L1_UPDATED_DATE,
  getdate() as  L2_LOADED_DATE,
  getdate() as L2_UPDATED_DATE
from {0}.catsweb.complaint_hazard  """.format(Catalog_Name_L1,Audit_Job_ID)

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
   
