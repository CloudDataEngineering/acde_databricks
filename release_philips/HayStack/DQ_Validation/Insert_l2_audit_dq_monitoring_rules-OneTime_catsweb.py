# Databricks notebook source
dbutils.widgets.text("ADF_Name", "", "ADF_Name")
Adf_Name = dbutils.widgets.get("ADF_Name")

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

#Source_Name='QDS'

# COMMAND ----------

# MAGIC %run /Shared/release/HayStack/Common_Functions/Haystack_Create_Audit_Tables-OneTime

# COMMAND ----------

#%run /Shared/release/HayStack/Common_Functions/Haystack_Merge

# COMMAND ----------

Unity_Catalog1="use catalog {}".format(Catalog_Name_L2)
spark.sql(Unity_Catalog1)
print(Unity_Catalog1)

# COMMAND ----------

#%sql
#Insert into dev_l2.qnr.l2_audit_dq_monitoring_rules
#values(0,'','','','','','','','','','' )

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType,LongType
DQ_Monitoring_Rules = Catalog_Name_L2+'.qnr.l2_audit_dq_monitoring_rules'

schema_master = StructType([
    StructField("DQ_ID",IntegerType(),True),
    StructField("Source_Name",StringType(),True),
    StructField("Source_Schema",StringType(),True),
    StructField("Source_Table",StringType(),True),
    StructField("Source_Filter",StringType(),True),
    StructField("Source_Columns",StringType(),True),
    StructField("Target_Schema",StringType(),True),
    StructField("Target_Table_Name",StringType(),True),
    StructField("Target_Filter",StringType(),True),
    StructField("Target_Columns",StringType(),True),
    StructField("ExtractionType",StringType(),True)
    ])

# COMMAND ----------

# MAGIC %md CATSWEB
# MAGIC

# COMMAND ----------

df_insert_query = spark.createDataFrame( [[28,'CatsWeb','CatsWeb','catsweb.complaint ','COMPLAINT_ID  not in (342,332,343,346)','Count(*)','qnr','complaint','APPLICATION_ID= 24500 and COMPLAINT_ID  not in (342,332,343,346)','Count(*)','Incremtal']],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )

# COMMAND ----------

df_insert_query = spark.createDataFrame( [[29,'CatsWeb','CatsWeb','catsweb.complaint_hazard ','1=1','Count(*)','qnr','complaint_hazard_catsweb','1=1','Count(*)','Incremtal']],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )

# COMMAND ----------

df_insert_query = spark.createDataFrame( [[30,'CatsWeb','CatsWeb','catsweb.complaint_hazard ','1=1','Count(*)','qnr','complaint_patient_outcome_catsweb','1=1','Count(*)','Incremtal']],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )

# COMMAND ----------

df_insert_query = spark.createDataFrame( [[31,'CatsWeb','CatsWeb','catsweb.complaint ) sl ',' rk =1 and COMPLAINT_ID  not in (342,332,343,346)',"COMPLAINT_ID,COMPLAINT_STATE,EVENT_DATE,DATE_OPENED,PRODUCT_FAMILY,OWNING_ENTITY_NAME,CATALOG_ITEM_ID,CATALOG_ITEM_NAME,PRODUCT_UDI,MANUFACTURE_DATE,EXPIRATION_DATE,EVENT_COUNTRY,BECOME_AWARE_DATE,REPORTED_PROBLEM_CODE_L1,REPORTED_PROBLEM_CODE_L2,REPORTED_PROBLEM_CODE_L3,DEVICE_USE_AT_TIME_OF_EVENT,IS_POTENTIAL_SAFETY_ALERT,IS_ALLEGATION_OF_INJURY_OR_DEATH,TYPE_OF_REPORTED_COMPLAINT,PROBLEM_SOURCE_CODE,PROBLEM_REASON_CODE,IS_CAPA_ADDTL_INVEST_REQUIRED,SOURCE_OF_COMPLAINT  from ( select COMPLAINT_ID	as	COMPLAINT_ID,STATE	as	COMPLAINT_STATE,cast(EVENT_DATE as date)    as  EVENT_DATE,CREATED_DATE	as	DATE_OPENED,DEVICE_TYPE	as	PRODUCT_FAMILY,DIVISION	as	OWNING_ENTITY_NAME,PRODUCT_MODEL_NR	as	CATALOG_ITEM_ID,PRODUCT_NAME	as	CATALOG_ITEM_NAME,UDI	as	PRODUCT_UDI,MANUFACTURED_DATE	as	MANUFACTURE_DATE,EXPIRATION_DATE	as	EXPIRATION_DATE,COUNTRY	as	EVENT_COUNTRY,TO_TIMESTAMP(COMPLAINT_AWARE_DATE, 'M/d/yyyy h:mm:ss a') as	BECOME_AWARE_DATE,REPORTED_PROBLEM_L1	as	REPORTED_PROBLEM_CODE_L1,REPORTED_PROBLEM_L2	as	REPORTED_PROBLEM_CODE_L2,PROBLEM_CODE	as	REPORTED_PROBLEM_CODE_L3,WHEN_DID_PROBLEM_OCCUR	as	DEVICE_USE_AT_TIME_OF_EVENT,case when  lower(POTENTIAL_SAFETY_REVIEW) ='no' then 'N' when  lower(POTENTIAL_SAFETY_REVIEW) ='yes' then 'Y' else Null end  	as	IS_POTENTIAL_SAFETY_ALERT,case when  lower(DID_DEATH_OR_SI_OCCUR) ='no' then 'N' when  lower(DID_DEATH_OR_SI_OCCUR )='yes' then 'Y' else Null end  	as	IS_ALLEGATION_OF_INJURY_OR_DEATH,TYPE_OF_REPORTABLE_EVENT	as	TYPE_OF_REPORTED_COMPLAINT,PROBABLE_CAUSE_CODE	as	PROBLEM_SOURCE_CODE,AS_ANALYZED_CODE	as	PROBLEM_REASON_CODE,case when  lower(IS_CAPA_REQUIRED) ='no' then 'N' when  lower(IS_CAPA_REQUIRED) ='yes' then 'Y' else Null end  	as	IS_CAPA_ADDTL_INVEST_REQUIRED,COMPLAINT_SOURCE	as	SOURCE_OF_COMPLAINT ,row_number() over ( partition by COMPLAINT_ID  order by LAST_UPDATED_DATE) as rk ",'qnr','complaint','APPLICATION_ID= 24500  and COMPLAINT_ID  not in (342,332,343,346)', 'COMPLAINT_ID,COMPLAINT_STATE, EVENT_DATE,DATE_OPENED,PRODUCT_FAMILY,OWNING_ENTITY_NAME, CATALOG_ITEM_ID,CATALOG_ITEM_NAME,PRODUCT_UDI,MANUFACTURE_DATE,EXPIRATION_DATE,EVENT_COUNTRY,BECOME_AWARE_DATE,REPORTED_PROBLEM_CODE_L1,REPORTED_PROBLEM_CODE_L2,REPORTED_PROBLEM_CODE_L3,DEVICE_USE_AT_TIME_OF_EVENT,IS_POTENTIAL_SAFETY_ALERT,IS_ALLEGATION_OF_INJURY_OR_DEATH,TYPE_OF_REPORTED_COMPLAINT,PROBLEM_SOURCE_CODE,PROBLEM_REASON_CODE,IS_CAPA_ADDTL_INVEST_REQUIRED,SOURCE_OF_COMPLAINT','Incremtal']],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )

# COMMAND ----------

df_insert_query = spark.createDataFrame( [[32,'CatsWeb','CatsWeb','catsweb.complaint_hazard','1=1'
                                           ,'cast(COMPLAINT_ID as bigint) as COMPLAINT_ID ,row_number() OVER(partition by COMPLAINT_ID  ORDER BY LAST_CREATED_DATE asc) as SEQ_NO,HAZARD_CATEGORY,HAZARD','qnr','complaint_hazard_catsweb','1=1','COMPLAINT_ID,SEQ_NO,HAZARD_CATEGORY,HAZARD','Incremtal']],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )

# COMMAND ----------

df_insert_query = spark.createDataFrame( [[33,'CatsWeb','CatsWeb','catsweb.complaint_hazard ','1=1',' COMPLAINT_ID ,  row_number() OVER(partition by COMPLAINT_ID  ORDER BY  LAST_CREATED_DATE asc)  as SEQ_NO,  SEVERITY as ACTUAL_HARM_SEVERITY ','qnr','complaint_patient_outcome_catsweb','1=1','COMPLAINT_ID,SEQ_NO,ACTUAL_HARM_SEVERITY','Incremtal']],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )

# COMMAND ----------

# MAGIC %md Legacy
# MAGIC

# COMMAND ----------

mon_rules_query ="""
Insert into {0}.qnr.l2_audit_dq_monitoring_rules
select '17','LegacyTW','Legacy_trackwise','N/A','PROJECT_ID in (2, 10, 1, 39)','N/A','qnr','complaint','APPLICATION_ID = 9375','N/A','onetime'
union all
select '18','LegacyTW','Legacy_trackwise','N/A','PROJECT_ID in (2, 10, 1, 39)','N/A','qnr','legacy_complaint_failure','APPLICATION_ID = 9375','N/A','onetime'
union all
select '19','LegacyTW','Legacy_trackwise','N/A','PROJECT_ID in (2, 10, 1, 39)','N/A','qnr','legacy_complaint_hazard','APPLICATION_ID = 9375','N/A','onetime'
union all
select '20','LegacyTW','Legacy_trackwise','vw_mv_complaint','1=1','*','qnr','cqb_ltw_complaint','1=1','*','onetime'
union all
select '21','LegacyTW','Legacy_trackwise','vw_mv_so_call_log','1=1','*','qnr','cqb_ltw_so_call_log','1=1','*','onetime'
union all
select '22','LegacyTW','Legacy_trackwise','vw_mv_aer_child','1=1','*','qnr','cqb_ltw_aer_child','1=1','*','onetime'
union all
select '23','LegacyTW','Legacy_trackwise','vw_mv_mdr','1=1','*','qnr','cqb_ltw_mdr','1=1','*','onetime'
union all
select '24','LegacyTW','Legacy_trackwise','vw_mv_mdv','1=1','*','qnr','cqb_ltw_mdv','1=1','*','onetime'
union all
select '25','LegacyTW','Legacy_trackwise','vw_mv_flex_field_text','1=1','*','qnr','cqb_ltw_flex_field_text','1=1','*','onetime'
union all
select '26','LegacyTW','Legacy_trackwise','vw_pr_file_path','1=1','*','qnr','cqb_ltw_pr_file_path','1=1','*','onetime'
union all
select '27','LegacyTW','Legacy_trackwise','vw_selection_type_values','1=1','*','qnr','cqb_ltw_selection_type_values','1=1','*','onetime' """.format(Catalog_Name_L2)
print(mon_rules_query)
spark.sql(mon_rules_query)

# COMMAND ----------


