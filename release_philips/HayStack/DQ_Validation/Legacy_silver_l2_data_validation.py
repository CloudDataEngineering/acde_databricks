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

dbutils.widgets.text("SourceName", "", "SourceName")
dbutils.widgets.text("ADF_Name", "", "ADF_Name")
dbutils.widgets.text("ADF_JobID", "", "ADF_JobID")

# COMMAND ----------

# DBTITLE 1,Parameters from ADF Job
System_SourceName = dbutils.widgets.get("SourceName")
ADF_Name = dbutils.widgets.get("ADF_Name")
ADF_JobID = dbutils.widgets.get("ADF_JobID")

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
	,cast(id  as int) as COMPLAINT_ID
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
""".format(Catalog_Name_L1,ADF_JobID)
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
    ,cast(pr.id as int) as COMPLAINT_ID
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
  ,cast(pr.id as int) as COMPLAINT_ID
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
  select cast(pr.Id  as int)  as COMPLAINT_ID
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
select cast( pr.id as int)   as COMPLAINT_ID
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
select cast(pr.id as int)   as COMPLAINT_ID, 
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
display(df_final_complaint.count())   

# COMMAND ----------

df_comp_tgt=spark.sql("""select 
						COMPLAINT_ID
				        ,APPLICATION_ID
				        ,COMPLAINT_STATE
				        ,EVENT_DATE
				        ,DATE_OPENED
				        ,PRODUCT_FAMILY
				        ,OWNING_ENTITY_NAME
				        ,CATALOG_ITEM_ID
				        ,CATALOG_ITEM_NAME
				        ,TRADE_ITEM_ID
				        ,TRADE_ITEM_NAME
				        ,DEVICE_IDENTIFIER
				        ,IS_THIRD_PARTY_DEVICE
				        ,MODEL_NUMBER
				        ,IS_MEDICAL_DEVICE
				        ,PRODUCT_UDI
				        ,MANUFACTURE_DATE
				        ,SHIP_DATE
				        ,EXPIRATION_DATE
				        ,REPORTER_COUNTRY
				        ,EVENT_COUNTRY
				        ,PHILIPS_NOTIFIED_DATE
				        ,INITIATE_DATE
				        ,BECOME_AWARE_DATE
				        ,COMPLAINT_SHORT_DESCRIPTION
				        ,COMPLAINT_CUSTOMER_DESCRIPTION
				        ,REPORTED_PROBLEM_CODE_L1
				        ,REPORTED_PROBLEM_CODE_L2
				        ,REPORTED_PROBLEM_CODE_L3
				        ,DEVICE_USE_AT_TIME_OF_EVENT
				        ,IS_PATIENT_USER_HARMED
				        ,IS_POTENTIAL_SAFETY_ALERT
				        ,IS_POTENTIAL_SAFETY_EVENT
				        ,IS_ALLEGATION_OF_INJURY_OR_DEATH
				        ,HAS_DEVICE_ALARMED
				        ,INCIDENT_KEY_WORDS
				        ,TYPE_OF_REPORTED_COMPLAINT
				        ,HAZARDOUS_SITUATION
				        ,COMPLAINT_LONG_DESCRIPTION
				        ,SOURCE_NOTES
				        ,COMMENTS
				        ,INVESTIGATION_SUMMARY
				        ,INVESTIGATION_NOTES
				        ,PROBLEM_SOURCE_CODE
				        ,PROBLEM_REASON_CODE
				        ,IS_CAPA_ADDTL_INVEST_REQUIRED
				        ,OTHER_RELEVANT_EVENT_INFO
				        ,PATIENT_ACTIONS_TAKEN
				        ,PROBLEM_SYMPTOMS_AND_FREQUENCY
				        ,ADDTL_INFORMATION
				        ,MEDICAL_RATIONALE
				        ,SOURCE_SYSTEM
				        ,SOURCE_OF_COMPLAINT
				        ,SOURCE_SERIAL_NUMBER
				        ,SERIAL_NUMBER
				        ,DATE_CREATED
				        ,DATE_CLOSED
				        ,SOURCE_CATALOG_ITEM_ID
				        ,SOURCE_CATALOG_ITEM_NAME
				        ,LOT_OR_BATCH_NUMBER
				        ,SOLUTION_FOR_THE_CUSTOMER
				        ,CUSTOMER_NUMBER
				        ,NO_FURTHER_INVEST_REQ_ON
				        ,SOURCE_EVENT_COUNTRY
				        ,ADDITIONAL_EVALUATION_COMMENTS
				        ,SYMPTOM_CODE_1
				        ,SYMPTOM_CODE_2
				        ,SYMPTOM_CODE_3
				        ,L1_LOADED_DATE
				        ,L1_UPDATED_DATE
from {0}.qnr.complaint where application_id ='9375' """.format(Catalog_Name_L2))

# COMMAND ----------

df_result_SL1_SL2=(df_final_complaint.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID')).subtract(df_comp_tgt.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID'))
df_result_SL2_SL1=(df_comp_tgt.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID')).subtract(df_final_complaint.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID'))
display(df_result_SL1_SL2)
display(df_result_SL2_SL1)
Run_Date= datetime.now()
Run_Date_st="'" + str(Run_Date) + "'"
Result_dict={}
Result_dict_key='legacy_complaint' +'_Validation'

if (df_result_SL2_SL1.count() == 0 and df_result_SL1_SL2.count() == 0 ):
    Result_status='Success' 
    print(Result_status)
    Query= "insert into {0}.qnr.l2_audit_dq_monitoring_rule_log (ADF_JobID,Run_Date,DQ_ID,Count_Silver_L1,Count_Silver_L2,Result_SL1_SL2) values ({1} ,{2} ,{3}  ,{4} ,{5} ,{6} )".format( Catalog_Name_L2 ,"'" + ADF_JobID +"'",  Run_Date_st , '17'   ,"'" + str(df_final_complaint.count())  +"'"  , "'" + str(df_comp_tgt.count()) +"'" , "'" + Result_status +"'"  )        
    spark.sql(Query)
    Result_dict[Result_dict_key]=Result_status
else:
    Result_status='Failed'
    print(Result_status)
    Query= "insert into {0}.qnr.l2_audit_dq_monitoring_rule_log (ADF_JobID,Run_Date,DQ_ID,Count_Silver_L1,Count_Silver_L2,Result_SL1_SL2) values ({1} ,{2} ,{3}  ,{4} ,{5},{6}   )".format( Catalog_Name_L2 ,"'" + ADF_JobID +"'",  Run_Date_st , '17'   ,"'" + str(df_final_complaint.count())  +"'"  , "'" + str(df_comp_tgt.count())  +"'", "'"+ Result_status +"'"  )         
    spark.sql(Query)
    Result_dict[Result_dict_key]=Result_status

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
""".format(Catalog_Name_L1,ADF_JobID)

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

df_comp_fail_tgt=spark.sql("""select * from {0}.qnr.complaint_failure_legacy """.format(Catalog_Name_L2))

# COMMAND ----------

df_comp_fail_cnt_res =df_failure_name.count()- df_comp_fail_tgt.count()
display(df_comp_fail_cnt_res)
df_result_SL1_SL2=(df_failure_name.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID')).subtract(df_comp_fail_tgt.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID'))
df_result_SL2_SL1=(df_comp_fail_tgt.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID')).subtract(df_failure_name.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID'))
display(df_result_SL1_SL2)
display(df_result_SL2_SL1)
Run_Date= datetime.now()
Run_Date_st="'" + str(Run_Date) + "'"
Result_dict_key='complaint_failure_legacy' +'_Validation'

if (df_result_SL2_SL1.count() == 0 and df_result_SL1_SL2.count() == 0 ):
    Result_status='Success' 
    print(Result_status)
    Query= "insert into {0}.qnr.l2_audit_dq_monitoring_rule_log (ADF_JobID,Run_Date,DQ_ID,Count_Silver_L1,Count_Silver_L2,Result_SL1_SL2) values ({1} ,{2} ,{3}  ,{4} ,{5} ,{6} )".format( Catalog_Name_L2 ,"'" + ADF_JobID +"'",  Run_Date_st , '18'   ,"'" + str(df_failure_name.count())  +"'"  , "'" + str(df_comp_fail_tgt.count()) +"'" , "'" + Result_status +"'"  )        
    spark.sql(Query)
    Result_dict[Result_dict_key]=Result_status
else:
    Result_status='Failed'
    print(Result_status)
    Query= "insert into {0}.qnr.l2_audit_dq_monitoring_rule_log (ADF_JobID,Run_Date,DQ_ID,Count_Silver_L1,Count_Silver_L2,Result_SL1_SL2) values ({1} ,{2} ,{3}  ,{4} ,{5},{6}   )".format( Catalog_Name_L2 ,"'" + ADF_JobID +"'",  Run_Date_st , '18'   ,"'" + str(df_failure_name.count())  +"'"  , "'" + str(df_comp_fail_tgt.count())  +"'", "'"+ Result_status +"'"  )         
    spark.sql(Query)
    Result_dict[Result_dict_key]=Result_status

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
""".format(Catalog_Name_L1,ADF_JobID)

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

df_comp_haz_tgt=spark.sql("""select * from {0}.qnr.complaint_hazard_legacy""".format(Catalog_Name_L2))

# COMMAND ----------

df_comp_haz_cnt_res =df_hazard_name.count()- df_comp_haz_tgt.count()
display(df_comp_haz_cnt_res)
df_result_SL1_SL2=(df_hazard_name.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID')).subtract(df_comp_haz_tgt.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID'))
df_result_SL2_SL1=(df_comp_haz_tgt.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID')).subtract(df_hazard_name.drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID'))
display(df_result_SL1_SL2)
display(df_result_SL2_SL1)
Run_Date= datetime.now()
Run_Date_st="'" + str(Run_Date) + "'"
Result_dict_key='complaint_hazard_legacy' +'_Validation'

if (df_result_SL2_SL1.count() == 0 and df_result_SL1_SL2.count() == 0 ):
    Result_status='Success' 
    print(Result_status)
    Query= "insert into {0}.qnr.l2_audit_dq_monitoring_rule_log (ADF_JobID,Run_Date,DQ_ID,Count_Silver_L1,Count_Silver_L2,Result_SL1_SL2) values ({1} ,{2} ,{3}  ,{4} ,{5} ,{6} )".format( Catalog_Name_L2 ,"'" + ADF_JobID +"'",  Run_Date_st , '19'   ,"'" + str(df_hazard_name.count())  +"'"  , "'" + str(df_comp_haz_tgt.count()) +"'" , "'" + Result_status +"'"  )        
    spark.sql(Query)
    Result_dict[Result_dict_key]=Result_status
else:
    Result_status='Failed'
    print(Result_status)
    Query= "insert into {0}.qnr.l2_audit_dq_monitoring_rule_log (ADF_JobID,Run_Date,DQ_ID,Count_Silver_L1,Count_Silver_L2,Result_SL1_SL2) values ({1} ,{2} ,{3}  ,{4} ,{5},{6}   )".format( Catalog_Name_L2 ,"'" + ADF_JobID +"'",  Run_Date_st , '19'   ,"'" + str(df_hazard_name.count())  +"'"  , "'" + str(df_comp_haz_tgt.count())  +"'", "'"+ Result_status +"'"  )         
    spark.sql(Query)
    Result_dict[Result_dict_key]=Result_status

# COMMAND ----------

# MAGIC %md
# MAGIC ####CQB Tables

# COMMAND ----------

pd_Max_Value=spark.sql("""select DQ_ID from {0}.qnr.l2_audit_dq_monitoring_rules where dq_id in ('20','21','22','23','24','25','26','27')
                        order by DQ_ID""".format(Catalog_Name_L2)).toPandas()['DQ_ID']
Max_Value_list=list(pd_Max_Value)
print(Max_Value_list)

# COMMAND ----------

Run_Date= datetime.now()
Run_Date_st="'" + str(Run_Date) + "'"
# Result_dict={}
for i in Max_Value_list:
            
            print(i)
            Result_status=''
            # featch DQ Rule from l2_audit_dq_monitoring_rules
            query="""select * from {2}.qnr.l2_audit_dq_monitoring_rules where DQ_ID = {0} and Source_Name = {1}""".format(i,"'" + System_SourceName + "'",Catalog_Name_L2 )
            print(query)
            Audit_df=spark.sql(query)
            # display(Audit_df)

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
            Result_dict_key=Source_Name+'_'+Target_Table_Name +'_Validation'
            
            #Create source and target query for Data validation for each rule  
            
            df_Source_catalog=spark.sql("Use CATALOG {}".format(Catalog_Name_L1))
            query_sql=" select {0} from legacy_trackwise.{2} where {3}  ;".format(Source_Columns,Source_Schema,Source_Table,Source_Filter)
            df_Source_value=spark.sql(query_sql).withColumnRenamed('ADLS_LOADED_DATE','L1_LOADED_DATE').withColumnRenamed('LAST_UPDATED_DATE','L1_UPDATED_DATE')


            df_Target_value= spark.sql("select {} from {}.{}.{} where {}".format(Target_Columns,Catalog_Name_L2,Target_Schema,Target_Table_Name,Target_Filter)).drop('L2_LOADED_DATE','L2_UPDATED_DATE','Audit_Job_ID')

            #Compare source and target DF
            df_result_SL1_SL2=df_Source_value.subtract(df_Target_value)
            df_result_SL2_SL1=df_Target_value.subtract(df_Source_value)


            #Add Count to variable 
            Count_Silver_L2=df_Target_value.count()
            Count_Silver_L1=df_Source_value.count()
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

