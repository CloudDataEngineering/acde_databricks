# Databricks notebook source
from delta.tables import *
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date, timedelta, datetime
from pyspark.sql import SQLContext

# COMMAND ----------

# MAGIC %md ###LegacyTW Silver L2

# COMMAND ----------

# DBTITLE 1,Audit  notebook
# MAGIC %run /Shared/release/HayStack/Common_Functions/Haystack_Audit

# COMMAND ----------

# DBTITLE 1,Parameters from ADF Job
dbutils.widgets.text("ADF_Name", "", "ADF_Name")
ADF_Name = dbutils.widgets.get("ADF_Name")

dbutils.widgets.text("Audit_Job_ID", "", "Audit_Job_ID")
Audit_Job_ID = dbutils.widgets.get("Audit_Job_ID")

# COMMAND ----------

if ADF_Name == 'az23d1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'dev_l2'
elif ADF_Name == 'az23t1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'dev_l2'
elif ADF_Name == 'az23q1-gf-quality-adf':
    Catalog_Name_L1 = 'qa_l1'
    Catalog_Name_L2 = 'qa_l2'
elif ADF_Name == 'az23p1-gf-quality-adf':
    Catalog_Name_L1 = 'prod_l1'
    Catalog_Name_L2 = 'prod_l2'

Source_Name = 'LegacyTW'
print(Catalog_Name_L1, Catalog_Name_L2, ADF_Name)

# COMMAND ----------

# MAGIC %md ####COMPLAINT

# COMMAND ----------

# MAGIC %md #####DF_PR

# COMMAND ----------

query_pr = """
  select 
	9375 as APPLICATION_ID
	,cast(pr.id as int) as COMPLAINT_ID
	,pr.NAME as COMPLAINT_SHORT_DESCRIPTION
	,pr.DATE_CREATED AS DATE_OPENED
	,null as TRADE_ITEM_ID
	,null as TRADE_ITEM_NAME
	,null as DEVICE_IDENTIFIER
	,null as IS_THIRD_PARTY_DEVICE
	,null as IS_MEDICAL_DEVICE
	,null as MANUFACTURE_DATE
	,null as PHILIPS_NOTIFIED_DATE
	,null as REPORTED_PROBLEM_CODE_L1
	,null as REPORTED_PROBLEM_CODE_L2
	,null as REPORTED_PROBLEM_CODE_L3
	,null as IS_PATIENT_USER_HARMED
	,null as IS_ALLEGATION_OF_INJURY_OR_DEATH
	,null as HAS_DEVICE_ALARMED
	,null as INCIDENT_KEY_WORDS
	,null as COMPLAINT_LONG_DESCRIPTION
	,null as PROBLEM_SOURCE_CODE
	,null as PROBLEM_REASON_CODE
	,null as IS_CAPA_ADDTL_INVEST_REQUIRED
	,null as PROBLEM_SYMPTOMS_AND_FREQUENCY
	,null as PATIENT_ACTIONS_TAKEN
	,null as ADDTL_INFORMATION
	,null as MEDICAL_RATIONALE
	,null as EXPIRATION_DATE
	,pr.ADLS_LOADED_DATE as L1_LOADED_DATE
    ,pr.LAST_UPDATED_DATE as L1_UPDATED_DATE
    ,'{1}' as Audit_Job_ID
    ,getdate() as L2_LOADED_DATE
    ,getdate() as L2_UPDATED_DATE
  from {0}.legacy_trackwise.vw_PR as pr 
  where PR.PROJECT_ID IN (2, 10, 1, 39)
""".format(Catalog_Name_L1,Audit_Job_ID)
df_pr = sqlContext.sql(query_pr).select(col("*"))

# COMMAND ----------

# MAGIC %md #####DF_PR_STATUS_TYPE

# COMMAND ----------

query_status_type =""" select 
9375 as APPLICATION_ID
,cast(pr.id as int) as COMPLAINT_ID
,pst.name as COMPLAINT_STATE
from {0}.legacy_trackwise.vw_PR as pr
join {0}.legacy_trackwise.PR_STATUS_TYPE pst
    on pr.status_type = pst.id
where PR.PROJECT_ID IN (2, 10, 1, 39)""".format(Catalog_Name_L1)

df_status_type = sqlContext.sql(query_status_type).select(col("COMPLAINT_ID"), \
                                                        col("APPLICATION_ID"), \
                                                        col("COMPLAINT_STATE")
                                                        )

# COMMAND ----------

# MAGIC %md #####DF_DATE_VALUE

# COMMAND ----------

query_date_value = """select *  from (
  select 9375 as APPLICATION_ID
    ,cast(pr.id as int)  as COMPLAINT_ID
    ,pr.name as COMPLAINT_SHORT_DESCRIPTION
    ,addtl.DATA_FIELD_ID
    ,addtl.DATE_VALUE as DATE_VALUE
  from {0}.legacy_trackwise.vw_PR as pr 
  join ( select * from (
    select pr_id,N_VALUE,data_field_id,date_updated,date_value,row_number() over(partition by pr_id,data_field_id,seq_no order by date_updated desc) as rown
  from {0}.legacy_trackwise.vw_PR_ADDTL_DATA )a  where rown =1)
  as addtl
  on pr.id = addtl.pr_id
  where PR.PROJECT_ID IN (2, 10, 1, 39)
  --and pr.id = 10672
  ) result
pivot
(
  max(DATE_VALUE)
  for (DATA_FIELD_ID)
  in (
    112,135,190
  )
)
""".format(Catalog_Name_L1)
#print(Query_DATE_VALUE)

df_date_value = sqlContext.sql(query_date_value).select(col("COMPLAINT_ID"), \
                                                        col("APPLICATION_ID"), \
                                                        col("112").cast('date').alias('EVENT_DATE'), \
                                                        col("135").alias('BECOME_AWARE_DATE'), \
                                                        col("190").alias('INITIATE_DATE'), \
                                                        col("COMPLAINT_SHORT_DESCRIPTION")
                                                        )
df_date_value.printSchema()


# COMMAND ----------

# MAGIC %md #####DF_DATE_VALUE_GRID
# MAGIC

# COMMAND ----------

query_date_value_grid  = """
select 9375 as APPLICATION_ID
    ,cast(pr.id as int)  as COMPLAINT_ID
    ,addtl.date_value as SHIP_DATE
    ,pr.name as COMPLAINT_SHORT_DESCRIPTION
from {0}.legacy_trackwise.PR pr 
join ( select * from (
    select id,pr_id,N_VALUE,data_field_id,date_updated,date_value,row_number() over(partition by pr_id,data_field_id,seq_no order by id desc) as rown
from {0}.legacy_trackwise.vw_PR_ADDTL_DATA )a  where rown =1)
 as addtl
on pr.id = addtl.pr_id
left join {0}.legacy_trackwise.vw_GRID_DATA grid
on pr.id = grid.pr_id
and grid.pr_addtl_data_id = addtl.id
where grid.data_field_id = 516
and grid.grid_id = 136
and grid.seq_no = 1
and PR.PROJECT_ID IN (2, 10, 1, 39)
""".format(Catalog_Name_L1)
df_date_value_grid = sqlContext.sql(query_date_value_grid)
df_date_value_grid.printSchema()

# COMMAND ----------

# MAGIC %md #####DF_TEXT_DATA
# MAGIC

# COMMAND ----------

query_text_data = """
select *  from (
  select 9375 as APPLICATION_ID
  ,cast(pr.id  as int) as COMPLAINT_ID
  ,TEXT_DATA
  ,PR_ELEMENT_TYPE
  ,pr.name as COMPLAINT_SHORT_DESCRIPTION 
  from {0}.legacy_trackwise.vw_pr as pr 
  join {0}.legacy_trackwise.vw_PR_ELEMENT as ELE
    on PR.id = ELE.PR_ID
  where PROJECT_ID IN (2, 10, 1, 39)) result
pivot
(
  max(TEXT_DATA)
  for (PR_ELEMENT_TYPE)
  in (
    1,2,7,8,17,20,75
  )
)
--where COMPLAINT_ID = 7988855
""".format(Catalog_Name_L1)

df_text_data = sqlContext.sql(query_text_data).select(col('COMPLAINT_ID'), \
                                                      col("APPLICATION_ID"), \
                                                      col('1').alias("COMPLAINT_CUSTOMER_DESCRIPTION"), \
                                                      col('2').alias("COMMENTS"), \
                                                      col('7').alias("INVESTIGATION_SUMMARY"), \
                                                      col('8').alias("SOURCE_NOTES"), \
                                                      col('17').alias("INVESTIGATION_NOTES"), \
                                                      col('20').alias("ADDITIONAL_EVALUATION_COMMENTS"), \
                                                      col('75').alias("OTHER_RELEVANT_EVENT_INFO"), \
                                                      col("COMPLAINT_SHORT_DESCRIPTION")
                                                      )
df_text_data.printSchema()                                                      

# COMMAND ----------

# MAGIC %md #####DF_NAME
# MAGIC

# COMMAND ----------

query_name = """
select *  from (
  select cast(pr.id as int)  as COMPLAINT_ID
  ,9375 AS APPLICATION_ID
  ,addtl.DATA_FIELD_ID
  ,addtype.NAME
  ,pr.name as COMPLAINT_SHORT_DESCRIPTION 
from {0}.legacy_trackwise.vw_pr as pr
join ( select * from (
    select pr_id,N_VALUE,data_field_id,date_updated,row_number() over(partition by pr_id,data_field_id,seq_no order by date_updated desc) as rown
from {0}.legacy_trackwise.vw_PR_ADDTL_DATA )a  where rown =1)
 as addtl
on pr.id = addtl.pr_id
left join {0}.legacy_trackwise.vw_ADDTL_TYPE addtype
on addtl.N_VALUE=addtype.ID
where 
pr.PROJECT_ID in (2, 10, 1, 39) --and pr.id = 11198--1200800
) result
pivot
(
  max(NAME)
  for (DATA_FIELD_ID)
  in (
    86,91,92,123,185,186,126,163,262,1261,1260,1265,1363
     )  
)
""".format(Catalog_Name_L1)

df_name = sqlContext.sql(query_name).select(
                                            col('COMPLAINT_ID'), \
                                            col('APPLICATION_ID'), \
                                            col('86').alias("CATALOG_ITEM_ID"),\
                                            col('91').alias("CATALOG_ITEM_NAME"),\
                                            col('92').alias("OWNING_ENTITY_NAME"),\
                                            col('123').alias("SYMPTOM_CODE_1"),\
                                            col('185').alias("SYMPTOM_CODE_2"),\
                                            col('186').alias("SYMPTOM_CODE_3"), \
                                            col('126').alias("TYPE_OF_REPORTED_COMPLAINT"),\
                                            col('163').alias("DEVICE_USE_AT_TIME_OF_EVENT"),\
                                            col('262').alias("EVENT_COUNTRY"),\
                                            when(col('1261') == 'No', 'N').\
                                            when(col('1261') == 'NO', 'N').\
                                            when(col('1261') == 'Yes','Y').\
                                            when(col('1261') == 'YES','Y').\
                                            otherwise(col('1261')).alias("IS_POTENTIAL_SAFETY_ALERT"),\
                                            when(col('1260') == 'No', 'N').\
                                            when(col('1260') == 'NO', 'N').\
                                            when(col('1260') == 'Yes','Y').\
                                            when(col('1260') == 'YES','Y').\
                                            otherwise(lit('null')).alias("IS_POTENTIAL_SAFETY_EVENT"),\
                                            col('1265').alias("REPORTER_COUNTRY"),\
                                            col('1363').alias("HAZARDOUS_SITUATION"), \
                                            col("COMPLAINT_SHORT_DESCRIPTION")
)
df_name.printSchema()
                                            

# COMMAND ----------

# MAGIC %md #####DF_NAME_GRID
# MAGIC

# COMMAND ----------

query_name_grid = """
select * from (
select cast(pr.id as int)  as COMPLAINT_ID
  ,9375 as APPLICATION_ID
  ,grid.DATA_FIELD_ID
  ,addtype.NAME
  ,pr.name as COMPLAINT_SHORT_DESCRIPTION 
from
  {0}.legacy_trackwise.vw_pr as pr
  left join ( select * from (
    select id,pr_id,N_VALUE,data_field_id,date_updated,row_number() over(partition by pr_id,data_field_id,seq_no order by id desc) as rown
  from {0}.legacy_trackwise.vw_PR_ADDTL_DATA )a  where rown =1)
  as addtl
  on pr.id = addtl.pr_id
  left join {0}.legacy_trackwise.vw_GRID_DATA grid
  on grid.PR_ADDTL_DATA_ID=addtl.ID and grid.GRID_ID = 193
  and grid.seq_no = 1
  left join {0}.legacy_trackwise.vw_ADDTL_TYPE addtype
  on addtl.N_VALUE=addtype.ID
  where pr.PROJECT_ID in (2, 10, 1, 39)
  ) result
  pivot
  (
    max(NAME)
  for (DATA_FIELD_ID)
  in (
    90,237
     )
  )
  """.format(Catalog_Name_L1)

df_name_grid = sqlContext.sql(query_name_grid).select(col('COMPLAINT_ID'), \
                                                        col('APPLICATION_ID'), \
                                                        col('90').alias("PRODUCT_FAMILY"), \
                                                        col('237').alias("MODEL_NUMBER"), \
                                                        col("COMPLAINT_SHORT_DESCRIPTION"))
df_name_grid.printSchema()                                                    

# COMMAND ----------

# MAGIC %md #####DF_S_VALUE
# MAGIC

# COMMAND ----------

query_s_name = """
select cast(pr.id  as int) as COMPLAINT_ID, 
    9375 as APPLICATION_ID,
    addtl.S_VALUE as PRODUCT_UDI,
    pr.name as COMPLAINT_SHORT_DESCRIPTION
from {0}.legacy_trackwise.vw_pr as pr 
join ( select * from (
    select pr_id,N_VALUE,data_field_id,date_updated,s_value,row_number() over(partition by pr_id,data_field_id,seq_no order by date_updated desc) as rown
from {0}.legacy_trackwise.vw_PR_ADDTL_DATA )a  where rown =1)
 as addtl
on pr.id = addtl.pr_id
where pr.PROJECT_ID in (2, 10, 1, 39)
and DATA_FIELD_ID=1346
and pr_id not in ('8349630','10506490')
union all
select 
COMPLAINT_ID,
APPLICATION_ID,
PRODUCT_UDI,
COMPLAINT_SHORT_DESCRIPTION
from 
(
select cast(pr.id as int)  as COMPLAINT_ID,
    9375 as APPLICATION_ID,
    addtl.S_VALUE as PRODUCT_UDI,
    pr.name as COMPLAINT_SHORT_DESCRIPTION
    ,row_number() over(partition by PR_ID,DATA_FIELD_ID,seq_no order by addtl.DATE_UPDATED desc ) as rown
from {0}.legacy_trackwise.vw_pr as pr 
join {0}.legacy_trackwise.vw_PR_ADDTL_DATA addtl
on pr.id = addtl.pr_id
where pr.PROJECT_ID in (2, 10, 1, 39)
and DATA_FIELD_ID=1346
and pr_id in ('8349630','10506490')
)a 
where a.rown=1
""".format(Catalog_Name_L1)
df_s_value = sqlContext.sql(query_s_name)
df_s_value.printSchema()

# COMMAND ----------

# MAGIC %md #####DF_COMPLAINT_FINAL
# MAGIC
# MAGIC

# COMMAND ----------

email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
name_regex = r"(Prof.\sdr.\s\w+|Prof.dr.\s\w+|Prof.dr.\w+|Prof.\sdr.\w+|Prof.\sDr.\s\w+|Prof.Dr.\s\w+|Prof.Dr.\w+|Prof.\sDr.\w+|Dr\.\s\w+|Mr\.\s\w+|Dr\.\w+|Mr\.\w+|Prof\.\w+|Prof\.\s\w+|Ms\.\w+|Ms\.\s\w+|Mrs\.\w+|Mrs\.\s\w+|Miss\s\w+|Herr\s\w+|Fräulein\s\w+|Frau\s\w+|Mr./Mrs.\s\w+|Mr./Mrs.\w+|Mr./Ms.\w+|Mr./Ms.\s\w+)"
#phone_regex = r"(\+\d{4,18})"
phone_regex =r"(\+\d{6,18})|(\+\d{1,3}\s\d{3}[-]\d{3}[-]\d{3,4})|(\+\d{1,3}\s[(]\d{3}[)]\s\d{3}[-]\d{3,4})|(\+\d{1,3}[-]\d{6,9})|(\+\d{1,3}[-]\d{2,5}\s\d{3,5})|(\+\d{4}[-]\d{3}[-]\d{4})|(\+\d{1,3}[-]\d{2,3}[-]\d{2,3}[-]\d{2,5})|(\+\d{3}[-]\d{3}[-]\d{4})|(\+\d{3}[-]\d{1}[-]\d{4}[-]\d{3})|(\+\d{3}[-]\d{2}[-]\d{7})|(\+\d{3}[-]\d{3}[-]\d{5})|(\+\d{1,3})"

df_final_complaint = df_pr.join(\
    df_date_value, df_pr.COMPLAINT_ID == df_date_value.COMPLAINT_ID,'left'). \
    join(df_date_value_grid, df_pr.COMPLAINT_ID == df_date_value_grid.COMPLAINT_ID,'left'). \
    join(df_text_data, df_pr.COMPLAINT_ID == df_text_data.COMPLAINT_ID,'left'). \
    join(df_name, df_pr.COMPLAINT_ID == df_name.COMPLAINT_ID,'left'). \
    join(df_name_grid, df_pr.COMPLAINT_ID == df_name_grid.COMPLAINT_ID,'left'). \
    join(df_status_type, df_pr.COMPLAINT_ID == df_status_type.COMPLAINT_ID,'left'). \
    join(df_s_value, df_pr.COMPLAINT_ID == df_s_value.COMPLAINT_ID,'left'). \
        select(
               df_pr.COMPLAINT_ID 
               ,df_pr.APPLICATION_ID
               ,df_status_type.COMPLAINT_STATE
               ,df_date_value.EVENT_DATE
               ,df_pr.DATE_OPENED
               ,df_name_grid.PRODUCT_FAMILY
               ,df_name.OWNING_ENTITY_NAME
               ,df_name.CATALOG_ITEM_ID
               ,df_name.CATALOG_ITEM_NAME
               ,df_pr.TRADE_ITEM_ID
               ,df_pr.TRADE_ITEM_NAME
               ,df_pr.DEVICE_IDENTIFIER
               ,df_pr.IS_THIRD_PARTY_DEVICE
               ,df_name_grid.MODEL_NUMBER
               ,df_pr.IS_MEDICAL_DEVICE
               ,df_s_value.PRODUCT_UDI
               ,df_pr.MANUFACTURE_DATE
               ,df_date_value_grid.SHIP_DATE
               ,df_pr.EXPIRATION_DATE
               ,df_name.REPORTER_COUNTRY
               ,df_name.EVENT_COUNTRY
               ,df_pr.PHILIPS_NOTIFIED_DATE
               ,df_date_value.INITIATE_DATE
               ,df_date_value.BECOME_AWARE_DATE
               ,regexp_replace(regexp_replace(regexp_replace(df_pr.COMPLAINT_SHORT_DESCRIPTION, phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("COMPLAINT_SHORT_DESCRIPTION")
               ,regexp_replace(regexp_replace(regexp_replace(df_text_data.COMPLAINT_CUSTOMER_DESCRIPTION, phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("COMPLAINT_CUSTOMER_DESCRIPTION")
            #    ,df_PR.COMPLAINT_SHORT_DESCRIPTION
            #    ,df_TEXT_DATA.COMPLAINT_CUSTOMER_DESCRIPTION
               ,df_pr.REPORTED_PROBLEM_CODE_L1
               ,df_pr.REPORTED_PROBLEM_CODE_L2
               ,df_pr.REPORTED_PROBLEM_CODE_L3
               ,df_name.DEVICE_USE_AT_TIME_OF_EVENT
               ,df_pr.IS_PATIENT_USER_HARMED
               ,df_name.IS_POTENTIAL_SAFETY_ALERT
               ,df_name.IS_POTENTIAL_SAFETY_EVENT
               ,df_pr.IS_ALLEGATION_OF_INJURY_OR_DEATH
               ,df_pr.HAS_DEVICE_ALARMED
               ,df_pr.INCIDENT_KEY_WORDS
               ,df_name.TYPE_OF_REPORTED_COMPLAINT
               ,df_name.HAZARDOUS_SITUATION
               ,df_pr.COMPLAINT_LONG_DESCRIPTION
               ,regexp_replace(regexp_replace(regexp_replace(df_text_data.SOURCE_NOTES, phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("SOURCE_NOTES")
               ,regexp_replace(regexp_replace(regexp_replace(df_text_data.COMMENTS, phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("COMMENTS")
               ,regexp_replace(regexp_replace(regexp_replace(df_text_data.INVESTIGATION_SUMMARY, phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("INVESTIGATION_SUMMARY")
               ,regexp_replace(regexp_replace(regexp_replace(df_text_data.INVESTIGATION_NOTES, phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("INVESTIGATION_NOTES")
            #    ,df_TEXT_DATA.SOURCE_NOTES
            #    ,df_TEXT_DATA.COMMENTS
            #    ,df_TEXT_DATA.INVESTIGATION_SUMMARY
            #    ,df_TEXT_DATA.INVESTIGATION_NOTES
               ,df_pr.PROBLEM_SOURCE_CODE
               ,df_pr.PROBLEM_REASON_CODE
               ,df_pr.IS_CAPA_ADDTL_INVEST_REQUIRED
               ,regexp_replace(regexp_replace(regexp_replace(df_text_data.OTHER_RELEVANT_EVENT_INFO, phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("OTHER_RELEVANT_EVENT_INFO")
            #    ,df_TEXT_DATA.OTHER_RELEVANT_EVENT_INFO
               ,df_pr.PATIENT_ACTIONS_TAKEN
               ,df_pr.PROBLEM_SYMPTOMS_AND_FREQUENCY
               ,df_pr.ADDTL_INFORMATION
               ,df_pr.MEDICAL_RATIONALE
               ,lit(None).alias('SOURCE_SYSTEM')
               ,lit(None).alias('SOURCE_OF_COMPLAINT')
               ,lit(None).alias('SOURCE_SERIAL_NUMBER')
               ,lit(None).alias('SERIAL_NUMBER')
               ,lit(None).alias('DATE_CREATED')
               ,lit(None).alias('DATE_CLOSED')
               ,lit(None).alias('SOURCE_CATALOG_ITEM_ID')
               ,lit(None).alias('SOURCE_CATALOG_ITEM_NAME')
               ,lit(None).alias('LOT_OR_BATCH_NUMBER')
               ,lit(None).alias('SOLUTION_FOR_THE_CUSTOMER')
               ,lit(None).alias('CUSTOMER_NUMBER')
               ,lit(None).alias('NO_FURTHER_INVEST_REQ_ON')
               ,lit(None).alias('SOURCE_EVENT_COUNTRY')
               # ,df_text_data.ADDITIONAL_EVALUATION_COMMENTS
               ,regexp_replace(regexp_replace(regexp_replace(df_text_data.ADDITIONAL_EVALUATION_COMMENTS, phone_regex, "{{PHONE}}"),email_regex,"{{EMAIL}}"),name_regex,"{{NAME}}").alias("ADDITIONAL_EVALUATION_COMMENTS")
               ,df_name.SYMPTOM_CODE_1
               ,df_name.SYMPTOM_CODE_2
               ,df_name.SYMPTOM_CODE_3
               ,df_pr.Audit_Job_ID
               ,df_pr.L1_LOADED_DATE
               ,df_pr.L1_UPDATED_DATE
               ,df_pr.L2_LOADED_DATE
               ,df_pr.L2_UPDATED_DATE
               )
# display(df_final_complaint.count())   

# COMMAND ----------

# DBTITLE 1,Append Complaint Data
Table_Name=Catalog_Name_L2+'.qnr.complaint'
var_Complaint_count = df_final_complaint.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,df_final_complaint)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)


# COMMAND ----------

# MAGIC %md ####COMPLAINT_FAILURE

# COMMAND ----------

query_failure_name = """
select *  from (
  select 
  cast(pr.Id as int) as COMPLAINT_ID
  ,9375 AS APPLICATION_ID
  ,1 as SEQ_NO
  ,addtl.DATA_FIELD_ID
  ,addtype.NAME
  ,pr.name as COMPLAINT_SHORT_DESCRIPTION
  ,pr.ADLS_LOADED_DATE as L1_LOADED_DATE
  ,pr.date_updated as L1_UPDATED_DATE
  ,'{1}' as Audit_Job_ID
  ,getdate() as L2_LOADED_DATE
  ,getdate() as L2_UPDATED_DATE 
  from {0}.legacy_trackwise.vw_pr as pr
join {0}.legacy_trackwise.vw_PR_ADDTL_DATA as addtl
on pr.id = addtl.pr_id
left join {0}.legacy_trackwise.vw_ADDTL_TYPE addtype
on addtl.N_VALUE=addtype.ID
where 
pr.PROJECT_ID in (2, 10, 1, 39) --and pr.id = 11198--1200800
) result
pivot
(
  max(NAME)
  for (DATA_FIELD_ID)
  in (
      156, 187, 157
     )  
)
""".format(Catalog_Name_L1,Audit_Job_ID)

df_failure_name = sqlContext.sql(query_failure_name).select(
                                            col('COMPLAINT_ID'), \
                                            col('APPLICATION_ID'), \
                                            col('SEQ_NO'), \
                                            col('156').alias("FAILURE_CODE"),\
                                            col('187').alias("FAILURE_CODE_DETAIL"),\
                                            col('157').alias("FAILURE_MONITORING_CODE"),\
                                            col('Audit_Job_ID'),\
                                            col('L1_LOADED_DATE'),\
                                            col('L1_UPDATED_DATE'),\
                                            col('L2_LOADED_DATE'),\
                                            col('L2_UPDATED_DATE')                                     
)

# COMMAND ----------

Table_Name=Catalog_Name_L2+'.qnr.complaint_failure_legacy'
var_Complaint_count = df_failure_name.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,df_failure_name)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)


# COMMAND ----------

# MAGIC %md ####COMPLAINT_HAZARD

# COMMAND ----------

query_hazard_name = """
select *  from (
  select cast(pr.Id as bigint) as COMPLAINT_ID
  ,9375 AS APPLICATION_ID
  ,1 as SEQ_NO
  ,addtl.DATA_FIELD_ID
  ,addtype.NAME
  ,pr.name as COMPLAINT_SHORT_DESCRIPTION
  ,'{1}' as Audit_Job_ID
  ,pr.ADLS_LOADED_DATE as L1_LOADED_DATE
  ,pr.date_updated as L1_UPDATED_DATE
  ,getdate() as L2_LOADED_DATE 
  ,getdate() as L2_UPDATED_DATE 
  from {0}.legacy_trackwise.vw_pr as pr
  join {0}.legacy_trackwise.vw_PR_ADDTL_DATA as addtl
    on pr.id = addtl.pr_id
  left join {0}.legacy_trackwise.ADDTL_TYPE addtype
    on addtl.N_VALUE=addtype.ID
  where pr.PROJECT_ID in (2, 10, 1, 39) --and pr.id = 11198--1200800
) result
pivot
(
  max(NAME)
  for (DATA_FIELD_ID)
  in (
      1361, 1362
     )  
)
""".format(Catalog_Name_L1,Audit_Job_ID)

df_hazard_name = sqlContext.sql(query_hazard_name).select(
                                            col('COMPLAINT_ID'), \
                                            col('APPLICATION_ID'), \
                                            col('SEQ_NO'), \
                                            col('1361').alias("HAZARD_CATEGORY"),\
                                            col('1362').alias("HAZARD"), \
                                            col('Audit_Job_ID'),\
                                            col('L1_LOADED_DATE'),\
                                            col('L1_UPDATED_DATE'),\
                                            col('L2_LOADED_DATE'),\
                                            col('L2_UPDATED_DATE')  
                                            
)
df_hazard_name.printSchema()

# COMMAND ----------

Table_Name=Catalog_Name_L2+'.qnr.complaint_hazard_legacy'
var_Complaint_count = df_hazard_name.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,df_hazard_name)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)


# COMMAND ----------

cqb_comp_query =spark.sql("""select * from {0}.legacy_trackwise.vw_mv_complaint""".format(Catalog_Name_L1))
cqb_comp_df =cqb_comp_query.withColumn("Audit_Job_ID",lit(Audit_Job_ID))\
                            .withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE")\
                            .withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")\
                            .withColumn("L2_LOADED_DATE",current_timestamp())\
                            .withColumn("L2_UPDATED_DATE",current_timestamp())
Table_Name=Catalog_Name_L2+'.qnr.cqb_ltw_complaint'
var_Complaint_count = cqb_comp_df.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,cqb_comp_df)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)



# COMMAND ----------

cqb_so_call_log_query =spark.sql("""select * from {0}.legacy_trackwise.vw_mv_so_call_log""".format(Catalog_Name_L1))
cqb_so_call_log_df =cqb_so_call_log_query.withColumn("Audit_Job_ID",lit(Audit_Job_ID))\
                            .withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE")\
                            .withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")\
                            .withColumn("L2_LOADED_DATE",current_timestamp())\
                            .withColumn("L2_UPDATED_DATE",current_timestamp())
Table_Name=Catalog_Name_L2+'.qnr.cqb_ltw_so_call_log'
var_Complaint_count = cqb_so_call_log_df.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,cqb_so_call_log_df)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)


# COMMAND ----------

cqb_aer_child_query =spark.sql("""select * from {0}.legacy_trackwise.vw_mv_aer_child""".format(Catalog_Name_L1))
cqb_aer_child_df =cqb_aer_child_query.withColumn("Audit_Job_ID",lit(Audit_Job_ID))\
                            .withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE")\
                            .withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")\
                            .withColumn("L2_LOADED_DATE",current_timestamp())\
                            .withColumn("L2_UPDATED_DATE",current_timestamp())
Table_Name=Catalog_Name_L2+'.qnr.cqb_ltw_aer_child'
var_Complaint_count = cqb_aer_child_df.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,cqb_aer_child_df)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)

# COMMAND ----------

cqb_mdr_query =spark.sql("""select * from {0}.legacy_trackwise.vw_mv_mdr""".format(Catalog_Name_L1))
cqb_mdr_df =cqb_mdr_query.withColumn("Audit_Job_ID",lit(Audit_Job_ID))\
                            .withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE")\
                            .withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")\
                            .withColumn("L2_LOADED_DATE",current_timestamp())\
                            .withColumn("L2_UPDATED_DATE",current_timestamp())
Table_Name=Catalog_Name_L2+'.qnr.cqb_ltw_mdr'
var_Complaint_count = cqb_mdr_df.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,cqb_mdr_df)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)

# COMMAND ----------

cqb_mdv_query =spark.sql("""select * from {0}.legacy_trackwise.vw_mv_mdv""".format(Catalog_Name_L1))
cqb_mdv_df =cqb_mdv_query.withColumn("Audit_Job_ID",lit(Audit_Job_ID))\
                            .withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE")\
                            .withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")\
                            .withColumn("L2_LOADED_DATE",current_timestamp())\
                            .withColumn("L2_UPDATED_DATE",current_timestamp())
Table_Name=Catalog_Name_L2+'.qnr.cqb_ltw_mdv'
var_Complaint_count = cqb_mdv_df.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,cqb_mdv_df)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)

# COMMAND ----------

cqb_flex_field_text_query =spark.sql("""select * from {0}.legacy_trackwise.vw_mv_flex_field_text""".format(Catalog_Name_L1))
cqb_flex_field_text_df =cqb_flex_field_text_query.withColumn("Audit_Job_ID",lit(Audit_Job_ID))\
                            .withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE")\
                            .withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")\
                            .withColumn("L2_LOADED_DATE",current_timestamp())\
                            .withColumn("L2_UPDATED_DATE",current_timestamp())
Table_Name=Catalog_Name_L2+'.qnr.cqb_ltw_flex_field_text'
var_Complaint_count = cqb_flex_field_text_df.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,cqb_flex_field_text_df)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)

# COMMAND ----------

cqb_pr_file_path_query =spark.sql("""select * from {0}.legacy_trackwise.vw_pr_file_path""".format(Catalog_Name_L1))
cqb_pr_file_path_df =cqb_pr_file_path_query.withColumn("Audit_Job_ID",lit(Audit_Job_ID))\
                            .withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE")\
                            .withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")\
                            .withColumn("L2_LOADED_DATE",current_timestamp())\
                            .withColumn("L2_UPDATED_DATE",current_timestamp())
Table_Name=Catalog_Name_L2+'.qnr.cqb_ltw_pr_file_path'
var_Complaint_count = cqb_pr_file_path_df.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,cqb_pr_file_path_df)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)

# COMMAND ----------

cqb_selection_type_values_query =spark.sql("""select * from {0}.legacy_trackwise.vw_selection_type_values""".format(Catalog_Name_L1))
cqb_selection_type_values_df =cqb_selection_type_values_query.withColumn("Audit_Job_ID",lit(Audit_Job_ID))\
                            .withColumnRenamed('ADLS_LOADED_DATE',"L1_LOADED_DATE")\
                            .withColumnRenamed('LAST_UPDATED_DATE',"L1_UPDATED_DATE")\
                            .withColumn("L2_LOADED_DATE",current_timestamp())\
                            .withColumn("L2_UPDATED_DATE",current_timestamp())
Table_Name=Catalog_Name_L2+'.qnr.cqb_ltw_selection_type_values'
var_Complaint_count = cqb_selection_type_values_df.count()
Audit_Start_date_Time =datetime.now()
Delta_Append(Table_Name,cqb_selection_type_values_df)
Audit_logging(None ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,'legacy' ,Table_Name,'Completed',var_Complaint_count,None,None,None)
