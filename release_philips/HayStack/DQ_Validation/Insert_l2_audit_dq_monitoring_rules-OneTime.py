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
Source_Name='QDS'

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

# DBTITLE 1,complaint
df_insert_query = spark.createDataFrame( [[1,'QDS','qds','qds.vw_rds_pr_1','PROJECT_ID in (16,17)','Count(*)','qnr','complaint','APPLICATION_ID = 17973','Count(*)','Incremtal']],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )

df_insert_query = spark.createDataFrame([[ 2,'QDS','qds','qds.vw_rds_pr_1 PR1 Left join qds.vw_pr_status_type as PR_S on PR1.Status_Type= PR_S.ID   LEFT JOIN  qds.vw_rds_pr_2 PR2  ON PR1.ID=PR2.ID  Left join  qds.vw_addtl_type ADT_379 on ADT_379.ID = PR1.DF_379  Left join  qds.vw_addtl_type ADT_84  on  ADT_84.ID = PR1.DF_84   Left join  qds.vw_addtl_type ADT_1286  on  ADT_1286.ID = PR2.DF_1286  Left join  qds.vw_addtl_type ADT_1156  on  ADT_1156.ID = PR1.DF_1156  Left join  qds.vw_addtl_type ADT_635  on  ADT_635.ID = PR1.DF_635 Left join  qds.vw_addtl_type ADT_662  on  ADT_662.ID = PR1.DF_662  Left join  qds.vw_addtl_type ADT_1249  on  ADT_1249.ID = PR2.DF_1249  Left join  qds.vw_addtl_type ADT_1250  on ADT_1250.ID = PR2.DF_1250  Left join  qds.vw_addtl_type ADT_1366  on  ADT_1366.ID = PR2.DF_1366  Left join  qds.vw_addtl_type ADT_460  on  ADT_460.ID = PR1.DF_460  Left join  qds.vw_addtl_type ADT_619  on  ADT_619.ID = PR1.DF_619  Left join  qds.vw_addtl_type ADT_624  on  ADT_624.ID = PR1.DF_624  Left join  qds.vw_addtl_type ADT_625  on  ADT_625.ID = PR1.DF_625  Left join  qds.vw_addtl_type ADT_1368   on ADT_1368.ID = PR2.DF_1368  Left join  qds.vw_addtl_type ADT_450 on  ADT_450.ID = PR1.DF_450 Left join  qds.vw_addtl_type ADT_1369  on  ADT_1369.ID = PR2.DF_1369  Left join  qds.vw_addtl_type ADT_654  on  ADT_654.ID = PR1.DF_654  Left join  qds.vw_addtl_type ADT_1052  on ADT_1052.ID = PR1.DF_1052  Left join  qds.vw_addtl_type ADT_1371  on  ADT_1371.ID = PR2.DF_1371  Left join  qds.vw_addtl_type ADT_1372  on ADT_1372.ID = PR2.DF_1372  Left join  qds.vw_addtl_type ADT_1293  on  ADT_1293.ID = PR2.DF_1293 Left join  qds.vw_addtl_type ADT_649  on  ADT_649.ID = PR1.DF_649  Left join  qds.vw_addtl_type ADT_646  on  ADT_646.ID = PR1.DF_646  Left join  qds.vw_addtl_type ADT_645  on  ADT_645.ID = PR1.DF_645  Left join  qds.vw_addtl_type ADT_1256  on  ADT_1256.ID = PR2.DF_1256','PR1.PROJECT_ID in (16,17)',"  PR1.ID  as COMPLAINT_ID ,  17973 as APPLICATION_ID,  PR_S.NAME as COMPLAINT_STATE, cast(DF_530 as DATE) as EVENT_DATE ,  PR1.DF_3 as DATE_OPENED, ADT_379.Name as PRODUCT_FAMILY ,  ADT_84.Name as OWNING_ENTITY_NAME ,  DF_1157 as CATALOG_ITEM_ID ,  DF_1162 as CATALOG_ITEM_NAME , DF_1137 as TRADE_ITEM_ID ,  DF_1154 as TRADE_ITEM_NAME ,  DF_574 as DEVICE_IDENTIFIER , case when lower(trim(ADT_1286.NAME))='no' then 'N' when lower(trim(ADT_1286.NAME))='yes' THEN 'y' ELSE NULL end as IS_THIRD_PARTY_DEVICE , DF_857 as MODEL_NUMBER ,  case when lower(trim(ADT_1156.NAME))='no' THEN 'N' when  lower(trim(ADT_1156.NAME))='yes' THEN 'Y'  else NULL END as IS_MEDICAL_DEVICE ,  DF_554 as PRODUCT_UDI, DF_534 as MANUFACTURE_DATE ,  DF_543 as SHIP_DATE ,  DF_531 as EXPIRATION_DATE ,  ADT_635.NAME  as REPORTER_COUNTRY ,  ADT_662.NAME as EVENT_COUNTRY ,  DF_535 as PHILIPS_NOTIFIED_DATE ,  DF_533 as INITIATE_DATE ,  DF_512 as BECOME_AWARE_DATE ,  ADT_1249.NAME as REPORTED_PROBLEM_CODE_L1,  ADT_1250.NAME as REPORTED_PROBLEM_CODE_L2,  ADT_1366.NAME as REPORTED_PROBLEM_CODE_L3,  ADT_460.NAME as DEVICE_USE_AT_TIME_OF_EVENT ,  case when lower(trim(ADT_619.NAME))='no' THEN 'N' WHEN lower(trim(ADT_619.NAME)) ='yes' THEN 'Y' ELSE NULL END as IS_PATIENT_USER_HARMED ,  case when lower(trim(ADT_624.NAME))='no' THEN 'N' when lower(trim(ADT_624.NAME))='yes' then 'Y' ELSE NULL END as IS_POTENTIAL_SAFETY_ALERT,  case when lower(trim(ADT_625.NAME))='no' THEN 'N' when lower(trim(ADT_625.NAME))='yes' THEN 'Y' ELSE NULL end as IS_POTENTIAL_SAFETY_EVENT,  case when lower(trim(ADT_1368.NAME))='no' THEN 'N' when lower(trim(ADT_1368.NAME))='yes' THEN 'Y' ELSE NULL end as IS_ALLEGATION_OF_INJURY_OR_DEATH,  case when lower(trim(ADT_450.NAME))='no' THEN 'N' when lower(trim(ADT_450.NAME))='yes' THEN 'Y' ELSE NULL end as  HAS_DEVICE_ALARMED,  ADT_1369.NAME as INCIDENT_KEY_WORDS,  ADT_654.NAME as TYPE_OF_REPORTED_COMPLAINT, ADT_1052.NAME as HAZARDOUS_SITUATION,  ADT_1371.NAME as PROBLEM_SOURCE_CODE,  ADT_1372.NAME as PROBLEM_REASON_CODE,  case when lower(trim(ADT_1293.NAME))='no' THEN 'N' when lower(trim(ADT_1293.NAME))='yes' THEN 'Y' ELSE NULL end as IS_CAPA_ADDTL_INVEST_REQUIRED,  ADT_649.NAME as SOURCE_SYSTEM,  ADT_646.NAME as SOURCE_OF_COMPLAINT, PR1.DF_493 as SOURCE_SERIAL_NUMBER, PR1.DF_489 as SERIAL_NUMBER, PR1.DATE_CREATED as DATE_CREATED, PR1.DATE_CLOSED as  DATE_CLOSED,  PR1.DF_1159 as SOURCE_CATALOG_ITEM_ID,  PR1.DF_1163 as SOURCE_CATALOG_ITEM_NAME, PR1.DF_593 as LOT_OR_BATCH_NUMBER, ADT_645.NAME as SOLUTION_FOR_THE_CUSTOMER,  PR1.DF_557 as CUSTOMER_NUMBER, PR1.DF_476 as NO_FURTHER_INVEST_REQ_ON, ADT_1256.NAME as SOURCE_EVENT_COUNTRY  ",'qnr','complaint','APPLICATION_ID = 17973','COMPLAINT_ID ,APPLICATION_ID,COMPLAINT_STATE,EVENT_DATE ,DATE_OPENED,PRODUCT_FAMILY ,OWNING_ENTITY_NAME ,CATALOG_ITEM_ID ,CATALOG_ITEM_NAME,TRADE_ITEM_ID ,TRADE_ITEM_NAME ,DEVICE_IDENTIFIER ,IS_THIRD_PARTY_DEVICE , MODEL_NUMBER ,IS_MEDICAL_DEVICE ,PRODUCT_UDI,MANUFACTURE_DATE ,SHIP_DATE ,EXPIRATION_DATE ,REPORTER_COUNTRY ,EVENT_COUNTRY ,PHILIPS_NOTIFIED_DATE ,INITIATE_DATE ,BECOME_AWARE_DATE ,REPORTED_PROBLEM_CODE_L1,REPORTED_PROBLEM_CODE_L2,REPORTED_PROBLEM_CODE_L3,DEVICE_USE_AT_TIME_OF_EVENT ,IS_PATIENT_USER_HARMED ,IS_POTENTIAL_SAFETY_ALERT,IS_POTENTIAL_SAFETY_EVENT,IS_ALLEGATION_OF_INJURY_OR_DEATH,HAS_DEVICE_ALARMED,INCIDENT_KEY_WORDS,TYPE_OF_REPORTED_COMPLAINT,HAZARDOUS_SITUATION,PROBLEM_SOURCE_CODE,PROBLEM_REASON_CODE,IS_CAPA_ADDTL_INVEST_REQUIRED,SOURCE_SYSTEM,SOURCE_OF_COMPLAINT,SOURCE_SERIAL_NUMBER,SERIAL_NUMBER,DATE_CREATED,DATE_CLOSED,SOURCE_CATALOG_ITEM_ID,SOURCE_CATALOG_ITEM_NAME,LOT_OR_BATCH_NUMBER,SOLUTION_FOR_THE_CUSTOMER,CUSTOMER_NUMBER,NO_FURTHER_INVEST_REQ_ON,SOURCE_EVENT_COUNTRY','Incremtal'
]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )





# COMMAND ----------

# DBTITLE 1,complaint_failure
df_insert_query = spark.createDataFrame( [[ 3,'QDS','qds','qds.vw_rds_pr_1 PR1 Inner join qds.vw_rds_grid_1144 GRD  	ON PR1.ID=GRD.PR_ID  Left join  qds.vw_addtl_type ADT_1148 	ON GRD.DF_1148 = ADT_1148.ID Left join  qds.vw_addtl_type ADT_1150 	ON GRD.DF_1150 = ADT_1150.ID Left join  qds.vw_addtl_type ADT_1151 	on GRD.DF_1151 = ADT_1151.ID','PROJECT_ID in (16,17)','Count(*)','qnr','complaint_failure','APPLICATION_ID = 17973','Count(*)','Overwrite'	]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )


df_insert_query = spark.createDataFrame( [[  4,'QDS','qds','qds.vw_rds_pr_1 PR1 Inner join qds.vw_rds_grid_1144 GRD  	ON PR1.ID=GRD.PR_ID  Left join  qds.vw_addtl_type ADT_1148 	ON GRD.DF_1148 = ADT_1148.ID Left join  qds.vw_addtl_type ADT_1150 	ON GRD.DF_1150 = ADT_1150.ID Left join  qds.vw_addtl_type ADT_1151 	on GRD.DF_1151 = ADT_1151.ID ','PROJECT_ID in (16,17)','    GRD.PR_ID   as COMPLAINT_ID ,	17973 as APPLICATION_ID,	GRD.SEQ_NO as SEQ_NO ,	ADT_1148.Name as FAILURE_CODE,	ADT_1150.Name as FAILURE_CODE_DETAIL,	ADT_1151.Name as FAILURE_MONITORING_CODE','qnr','complaint_failure','APPLICATION_ID = 17973','COMPLAINT_ID ,APPLICATION_ID,SEQ_NO ,FAILURE_CODE,FAILURE_CODE_DETAIL,FAILURE_MONITORING_CODE','Overwrite'
                                           ]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )

# COMMAND ----------

# DBTITLE 1,complaint_hazard
df_insert_query = spark.createDataFrame( [[ 5,'QDS','qds','qds.vw_rds_pr_1 PR1 Inner join qds.vw_rds_grid_1132 GRD 	ON PR1.ID=GRD.PR_ID Left join qds.vw_addtl_type   ADT_1134 	ON GRD.DF_1134=ADT_1134.ID Left join qds.vw_addtl_type   ADT_1133 	ON GRD.DF_1133=ADT_1133.ID ','PROJECT_ID in (16,17)','count(*)','qnr','complaint_hazard','APPLICATION_ID = 17973','count(*)','Overwrite' ]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )                                         
                                
df_insert_query = spark.createDataFrame( [[  6,'QDS','qds','qds.vw_rds_pr_1 PR1 Inner join qds.vw_rds_grid_1132 GRD  	ON PR1.ID=GRD.PR_ID Left join qds.vw_addtl_type   ADT_1134 	ON GRD.DF_1134=ADT_1134.ID Left join qds.vw_addtl_type   ADT_1133 	ON GRD.DF_1133=ADT_1133.ID ','PROJECT_ID in (16,17)', 'GRD.PR_ID  as COMPLAINT_ID ,17973 as APPLICATION_ID,GRD.SEQ_NO as SEQ_NO ,ADT_1134.Name as HAZARD_CATEGORY,ADT_1133.Name as HAZARD','qnr','complaint_hazard','APPLICATION_ID = 17973','COMPLAINT_ID , APPLICATION_ID,SEQ_NO ,HAZARD_CATEGORY,HAZARD','Overwrite']],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID")

# COMMAND ----------

# DBTITLE 1,complaint_patient_outcome
df_insert_query = spark.createDataFrame( [[ 7,'QDS','qds','	qds.vw_rds_pr_1 PR1 Inner join qds.vw_rds_grid_1113 GRD  	ON PR1.ID=GRD.PR_ID Left join qds.vw_addtl_type   ADT_1126 	ON GRD.DF_1126=ADT_1126.ID','PROJECT_ID in (16,17)','count(*) ','qnr','complaint_patient_outcome','APPLICATION_ID = 17973','count(*)','Overwrite' 	 ]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )                                         

df_insert_query = spark.createDataFrame( [[ 8,'QDS','qds','	qds.vw_rds_pr_1 PR1 Inner join qds.vw_rds_grid_1113 GRD  	ON PR1.ID=GRD.PR_ID Left join qds.vw_addtl_type   ADT_1126 	ON GRD.DF_1126=ADT_1126.ID ','PROJECT_ID in (16,17)','GRD.PR_ID  as COMPLAINT_ID , 17973 as APPLICATION_ID, GRD.SEQ_NO as SEQ_NO , ADT_1126.NAME as ACTUAL_HARM_SEVERITY ','qnr','complaint_patient_outcome','APPLICATION_ID = 17973','COMPLAINT_ID,APPLICATION_ID,SEQ_NO,ACTUAL_HARM_SEVERITY','Overwrite'
]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" ) 

# COMMAND ----------

# DBTITLE 1,complaint_part_used
df_insert_query = spark.createDataFrame( [[ 9,'QDS','qds','qds.vw_rds_pr_1 as PR1 Inner join qds.vw_rds_grid_1266 as GRD    on GRD.PR_ID = PR1.ID    Left join qds.vw_addtl_type ADL_1117   on  ADL_1117.ID = GRD.DF_1117 Left join qds.vw_addtl_type ADL_1118   on  ADL_1118.ID = GRD.DF_1118 Left join qds.vw_addtl_type ADL_1119   on  ADL_1119.ID = GRD.DF_1119','PROJECT_ID in (16,17)','count(*) ','qnr','complaint_part_used','APPLICATION_ID = 17973','count(*)','Overwrite'
]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )   

df_insert_query = spark.createDataFrame( [[  10,'QDS','qds', 'qds.vw_rds_pr_1 as PR1 Inner join qds.vw_rds_grid_1266 as GRD    on GRD.PR_ID = PR1.ID   Left join qds.vw_addtl_type ADL_1117   on  ADL_1117.ID = GRD.DF_1117 Left join qds.vw_addtl_type ADL_1118   on  ADL_1118.ID = GRD.DF_1118 Left join qds.vw_addtl_type ADL_1119   on  ADL_1119.ID = GRD.DF_1119','PROJECT_ID in (16,17)',
 'GRD.PR_ID as COMPLAINT_ID ,   17973 as APPLICATION_ID,   GRD.SEQ_NO  as SEQ_NO,   GRD.DF_1268 as CONSUMED_12NC,   GRD.DF_1270 as CONSUMED_PART_DESCRIPTION,   GRD.DF_1271 as CONSUMED_SERIAL_NUMBER,   GRD.DF_237 as QUANTITY ,   ADL_1117.NAME as PART_CLASSIFICATION,  GRD.DF_1273 as DEFECTIVE_12NC,   GRD.DF_1275 as DEFECTIVE_PART_DESCRIPTION,   GRD.DF_1276 as DEFECTIVE_SERIAL_NUMBER,  ADL_1118.NAME as DEFECTIVE_PART_FAILURE_REASON,   ADL_1119.NAME as DEFECTIVE_PART_CATEGORY'   ,'qnr','complaint_part_used','APPLICATION_ID = 17973',
  'COMPLAINT_ID , APPLICATION_ID,SEQ_NO, CONSUMED_12NC,CONSUMED_PART_DESCRIPTION, CONSUMED_SERIAL_NUMBER,QUANTITY , PART_CLASSIFICATION,DEFECTIVE_12NC, DEFECTIVE_PART_DESCRIPTION, DEFECTIVE_SERIAL_NUMBER, DEFECTIVE_PART_FAILURE_REASON,DEFECTIVE_PART_CATEGORY','Overwrite'
]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )   


# COMMAND ----------

# DBTITLE 1,complaint_duplicate
df_insert_query = spark.createDataFrame( [[  11,'QDS','qds','qds.vw_rds_pr_1 as PR1 Inner join qds.vw_rds_ref_record_510 as GRD  	on GRD.PR_ID = PR1.ID  ','PROJECT_ID in (16,17)','count(*) ','qnr','complaint_duplicate','APPLICATION_ID = 17973','count(*)','Overwrite'
	]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )   

df_insert_query = spark.createDataFrame( [[  12,'QDS','qds','qds.vw_rds_pr_1 as PR1 Inner join qds.vw_rds_ref_record_510 as GRD  	on GRD.PR_ID = PR1.ID  ','PROJECT_ID in (16,17)',   'GRD.PR_ID as COMPLAINT_ID , 17973 as APPLICATION_ID, DF_510  as COMPLAINT_ID_DUPLICATE, 17973 as APPLICATION_ID_DUPLICATE' ,'qnr','complaint_duplicate','APPLICATION_ID = 17973', 'COMPLAINT_ID , APPLICATION_ID, COMPLAINT_ID_DUPLICATE,  APPLICATION_ID_DUPLICATE', 'Overwrite'
]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )   

# COMMAND ----------

# DBTITLE 1,complaint_evaluation_result
df_insert_query = spark.createDataFrame( [[ 13,'QDS','qds','qds.vw_rds_pr_1 as PR1 Inner join qds.vw_rds_grid_1326 as GRD    on GRD.PR_ID = PR1.ID     Left join qds.vw_addtl_type ADL1   on  ADL1.ID = GRD.DF_1327 Left join qds.vw_addtl_type ADL2   on  ADL2.ID = GRD.DF_1328 Left join qds.vw_addtl_type ADL3    on  ADL3.ID = GRD.DF_1329 ','PROJECT_ID in (16,17)','count(*) ','qnr','complaint_evaluation_result','APPLICATION_ID = 17973','count(*)','Overwrite' 
  ]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )  

df_insert_query = spark.createDataFrame( [[ 14,'QDS','qds','qds.vw_rds_pr_1 as PR1 Inner join qds.vw_rds_grid_1326 as GRD    on GRD.PR_ID = PR1.ID     Left join qds.vw_addtl_type ADL1   on  ADL1.ID = GRD.DF_1327 Left join qds.vw_addtl_type ADL2   on  ADL2.ID = GRD.DF_1328 Left join qds.vw_addtl_type ADL3    on  ADL3.ID = GRD.DF_1329','PROJECT_ID in (16,17)', 'GRD.PR_ID as COMPLAINT_ID , 17973 as APPLICATION_ID, GRD.SEQ_NO  as SEQ_NO, ADL1.NAME as EVALUATION_RESULT_CODE_L1, ADL2.NAME as EVALUATION_RESULT_CODE_L2, ADL3.NAME as EVALUATION_RESULT_CODE_L3 '
,'qnr','complaint_evaluation_result','APPLICATION_ID = 17973',
'COMPLAINT_ID ,APPLICATION_ID,SEQ_NO,EVALUATION_RESULT_CODE_L1,EVALUATION_RESULT_CODE_L2,EVALUATION_RESULT_CODE_L3',
'Overwrite'
]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )  

# COMMAND ----------

# DBTITLE 1,Complaint Component
df_insert_query = spark.createDataFrame( [[ 15,'QDS','qds','qds.vw_rds_pr_1 as PR1 Inner join qds.vw_rds_grid_1332 as GRD  on GRD.PR_ID = PR1.ID   Left join qds.vw_addtl_type ADL1 on  ADL1.ID = GRD.DF_1333 Left join qds.vw_addtl_type ADL2 on  ADL2.ID = GRD.DF_1334  Left join qds.vw_addtl_type ADL3   on  ADL3.ID = GRD.DF_1335 ','PROJECT_ID in (16,17)','count(*) ','qnr','complaint_component','APPLICATION_ID = 17973','count(*)','Overwrite'
 ]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )  


df_insert_query = spark.createDataFrame( [[ 16,'QDS','qds','qds.vw_rds_pr_1 as PR1 Inner join qds.vw_rds_grid_1332 as GRD    on GRD.PR_ID = PR1.ID    Left join qds.vw_addtl_type ADL1   on  ADL1.ID = GRD.DF_1333 Left join qds.vw_addtl_type ADL2   on  ADL2.ID = GRD.DF_1334 Left join qds.vw_addtl_type ADL3    on  ADL3.ID = GRD.DF_1335' ,'PROJECT_ID in (16,17)',' GRD.PR_ID as COMPLAINT_ID , 17973 as APPLICATION_ID, GRD.SEQ_NO as SEQ_NO, ADL1.NAME as COMPONENT_CODE_L1, ADL2.NAME as COMPONENT_CODE_L2, ADL3.NAME as COMPONENT_CODE_L3'
,'qnr','complaint_component','APPLICATION_ID = 17973',
'COMPLAINT_ID,APPLICATION_ID,SEQ_NO,COMPONENT_CODE_L1,COMPONENT_CODE_L2,COMPONENT_CODE_L3',
'Overwrite'
]],schema_master)
delta_merge_all(DQ_Monitoring_Rules,df_insert_query,"DQ_ID" )  

