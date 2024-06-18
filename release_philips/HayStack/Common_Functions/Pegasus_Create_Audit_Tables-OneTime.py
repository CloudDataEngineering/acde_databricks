# Databricks notebook source
# MAGIC %sql
# MAGIC --truncate table dev_wb.gf_quality.g_audit_master

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType,LongType

# COMMAND ----------

# MAGIC %md ####Audit Master Table

# COMMAND ----------

# MAGIC %run /Shared/release/HayStack/Common_Functions/Haystack_G_Merge

# COMMAND ----------

# MAGIC %md #####Audit Master Data Entry - QDS

# COMMAND ----------

dbutils.widgets.text("ADF_Name", "", "ADF_Name")
ENV = dbutils.widgets.get("ADF_Name")

# COMMAND ----------

#############################################################################################
# Based on  Environment name i.e. Development, Test, QA,PROD. Catalog name will be set      #
#############################################################################################
if ENV == 'az23d1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_WB = 'dev_wb'
elif ENV == 'az23q1-gf-quality-adf':
    Catalog_Name_L1 = 'qa_l1'
    Catalog_Name_WB = 'qa_wb'
elif ENV == 'az23t1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_WB = 'test_wb'
elif ENV == 'az23p1-gf-quality-adf':
    Catalog_Name_L1 = 'prod_l1'
    Catalog_Name_WB = 'prod_wb'    
Source_Name='Pegasus'

# COMMAND ----------

 
Audit_Master=Catalog_Name_WB+'.gf_quality.g_audit_master'

schema_master = StructType([
    StructField("Audit_Master_ID",IntegerType(),True),
    StructField("Audit_Source_Name",StringType(),True),
    StructField("Audit_Table_Name",StringType(),True),
    StructField("Audit_Load_Type",StringType(),True),
    StructField("Audit_Merge_Key",StringType(),True),
    StructField("Audit_Update_Key",StringType(),True)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC QDs Audit Master insert

# COMMAND ----------

df_g_address	= spark.createDataFrame([[	1	,'PegasusQDS','g_address','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_addtl_type	= spark.createDataFrame([[	2	,'PegasusQDS','g_addtl_type','Full','ID','Date_Updated'] ],schema_master) 	
df_g_data_field_type	= spark.createDataFrame([[	3	,'PegasusQDS','g_data_field_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_data_fields	= spark.createDataFrame([[	4	,'PegasusQDS','g_data_fields','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_data_fields_edited	= spark.createDataFrame([[	5	,'PegasusQDS','g_data_fields_edited','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_department	= spark.createDataFrame([[	6	,'PegasusQDS','g_department','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_division_type	= spark.createDataFrame([[	7	,'PegasusQDS','g_division_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_entity	= spark.createDataFrame([[	8	,'PegasusQDS','g_entity','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_entity_element	= spark.createDataFrame([[	9	,'PegasusQDS','g_entity_element','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_enty_element_type	= spark.createDataFrame([[	10	,'PegasusQDS','g_enty_element_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_generic_short_text	= spark.createDataFrame([[	11	,'PegasusQDS','g_generic_short_text','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_grid_fields	= spark.createDataFrame([[	12	,'PegasusQDS','g_grid_fields','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_group_fields	= spark.createDataFrame([[	13	,'PegasusQDS','g_group_fields','Full','ID','Date_Updated'] ],schema_master) 	
df_g_group_member	= spark.createDataFrame([[	14	,'PegasusQDS','g_group_member','Full','ID','Date_Updated'] ],schema_master) 	
df_g_group_type	= spark.createDataFrame([[	15	,'PegasusQDS','g_group_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_language	= spark.createDataFrame([[	16	,'PegasusQDS','g_language','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_login	= spark.createDataFrame([[	17	,'PegasusQDS','g_login','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_object_type	= spark.createDataFrame([[	18	,'PegasusQDS','g_object_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_object_value	= spark.createDataFrame([[	19	,'PegasusQDS','g_object_value','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_person	= spark.createDataFrame([[	20	,'PegasusQDS','g_person','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_person_element	= spark.createDataFrame([[	21	,'PegasusQDS','g_person_element','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_person_relation	= spark.createDataFrame([[	22	,'PegasusQDS','g_person_relation','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_person_role_type	= spark.createDataFrame([[	23	,'PegasusQDS','g_person_role_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_activity	= spark.createDataFrame([[	24	,'PegasusQDS','g_pr_activity','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_activity_type	= spark.createDataFrame([[	25	,'PegasusQDS','g_pr_activity_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_addtl_data_sec	= spark.createDataFrame([[	26	,'PegasusQDS','g_pr_addtl_data_sec','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_category_type	= spark.createDataFrame([[	27	,'PegasusQDS','g_pr_category_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_element_type	= spark.createDataFrame([[	28	,'PegasusQDS','g_pr_element_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_more_info_type	= spark.createDataFrame([[	29	,'PegasusQDS','g_pr_more_info_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_opened_type	= spark.createDataFrame([[	30	,'PegasusQDS','g_pr_opened_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_priority_type	= spark.createDataFrame([[	31	,'PegasusQDS','g_pr_priority_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_severity_type	= spark.createDataFrame([[	32	,'PegasusQDS','g_pr_severity_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_state_node	= spark.createDataFrame([[	33	,'PegasusQDS','g_pr_state_node','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_pr_status_type	= spark.createDataFrame([[	34	,'PegasusQDS','g_pr_status_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_project	= spark.createDataFrame([[	35	,'PegasusQDS','g_project','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_project_member	= spark.createDataFrame([[	36	,'PegasusQDS','g_project_member','Full','ID','Date_Updated'] ],schema_master) 	
df_g_project_member_person_role	= spark.createDataFrame([[	37	,'PegasusQDS','g_project_member_person_role','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_prsn_element_type	= spark.createDataFrame([[	38	,'PegasusQDS','g_prsn_element_type','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_rds_etl_history	= spark.createDataFrame([[	39	,'PegasusQDS','g_rds_etl_history','Inc','ID','Date_Updated'] ],schema_master) 	
 


# COMMAND ----------

df_g_rds_failed_prs	= spark.createDataFrame([[	40	,'PegasusQDS','g_rds_failed_prs','Inc','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_157	= spark.createDataFrame([[	41	,'PegasusQDS','g_rds_grid_157','Full','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_194	= spark.createDataFrame([[	42	,'PegasusQDS','g_rds_grid_194','Full','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_282	= spark.createDataFrame([[	43	,'PegasusQDS','g_rds_grid_282','Full','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_338	= spark.createDataFrame([[	44	,'PegasusQDS','g_rds_grid_338','Full','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_353	= spark.createDataFrame([[	45	,'PegasusQDS','g_rds_grid_353','Full','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_358	= spark.createDataFrame([[	46	,'PegasusQDS','g_rds_grid_358','Full','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_376	= spark.createDataFrame([[	47	,'PegasusQDS','g_rds_grid_376','Full','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_380	= spark.createDataFrame([[	48	,'PegasusQDS','g_rds_grid_380','Full','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_393	= spark.createDataFrame([[	49	,'PegasusQDS','g_rds_grid_393','Full','ID','Date_Updated'] ],schema_master) 	
df_g_rds_grid_418= spark.createDataFrame([[50,'PegasusQDS','g_rds_grid_418','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_421= spark.createDataFrame([[51,'PegasusQDS','g_rds_grid_421','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_443= spark.createDataFrame([[52,'PegasusQDS','g_rds_grid_443','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_454= spark.createDataFrame([[53,'PegasusQDS','g_rds_grid_454','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_493= spark.createDataFrame([[54,'PegasusQDS','g_rds_grid_493','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_530= spark.createDataFrame([[55,'PegasusQDS','g_rds_grid_530','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_617= spark.createDataFrame([[56,'PegasusQDS','g_rds_grid_617','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_626= spark.createDataFrame([[57,'PegasusQDS','g_rds_grid_626','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_647= spark.createDataFrame([[58,'PegasusQDS','g_rds_grid_647','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_680= spark.createDataFrame([[59,'PegasusQDS','g_rds_grid_680','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_772= spark.createDataFrame([[60,'PegasusQDS','g_rds_grid_772','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_814= spark.createDataFrame([[61,'PegasusQDS','g_rds_grid_814','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_grid_966= spark.createDataFrame([[62,'PegasusQDS','g_rds_grid_966','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_group_type_viewable_flds= spark.createDataFrame([[63,'PegasusQDS','g_rds_group_type_viewable_flds','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_138= spark.createDataFrame([[64,'PegasusQDS','g_rds_multi_selection_138','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_146= spark.createDataFrame([[65,'PegasusQDS','g_rds_multi_selection_146','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_150= spark.createDataFrame([[66,'PegasusQDS','g_rds_multi_selection_150','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_259= spark.createDataFrame([[67,'PegasusQDS','g_rds_multi_selection_259','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_308= spark.createDataFrame([[68,'PegasusQDS','g_rds_multi_selection_308','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_451= spark.createDataFrame([[69,'PegasusQDS','g_rds_multi_selection_451','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_630= spark.createDataFrame([[70,'PegasusQDS','g_rds_multi_selection_630','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_719= spark.createDataFrame([[71,'PegasusQDS','g_rds_multi_selection_719','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_724= spark.createDataFrame([[72,'PegasusQDS','g_rds_multi_selection_724','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_selection_869= spark.createDataFrame([[73,'PegasusQDS','g_rds_multi_selection_869','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_text_331= spark.createDataFrame([[74,'PegasusQDS','g_rds_multi_text_331','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_multi_text_901= spark.createDataFrame([[75,'PegasusQDS','g_rds_multi_text_901','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_pr_1= spark.createDataFrame([[76,'PegasusQDS','g_rds_pr_1','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_ref_record_202= spark.createDataFrame([[77,'PegasusQDS','g_rds_ref_record_202','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_ref_record_258= spark.createDataFrame([[78,'PegasusQDS','g_rds_ref_record_258','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_ref_record_587= spark.createDataFrame([[79,'PegasusQDS','g_rds_ref_record_587','Inc','ID','Date_Updated'] ],schema_master) 
df_g_rds_security_gc= spark.createDataFrame([[80,'PegasusQDS','g_rds_security_gc','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_security_va= spark.createDataFrame([[81,'PegasusQDS','g_rds_security_va','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_security_vb= spark.createDataFrame([[82,'PegasusQDS','g_rds_security_vb','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_security_vgc= spark.createDataFrame([[83,'PegasusQDS','g_rds_security_vgc','Full','ID','Date_Updated'] ],schema_master) 
df_g_rds_snapshot= spark.createDataFrame([[84,'PegasusQDS','g_rds_snapshot','Full','ID','Date_Updated'] ],schema_master) 
df_g_user_group= spark.createDataFrame([[85,'PegasusQDS','g_user_group','Inc','ID','Date_Updated'] ],schema_master)

# COMMAND ----------

delta_g_merge_all(Audit_Master,	df_g_address	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_addtl_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_data_field_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_data_fields	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_data_fields_edited	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_department	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_division_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_entity	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_entity_element	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_enty_element_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_generic_short_text	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_grid_fields	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_group_fields	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_group_member	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_group_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_language	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_login	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_object_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_object_value	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_person	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_person_element	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_person_relation	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_person_role_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_activity	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_activity_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_addtl_data_sec	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_category_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_element_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_more_info_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_opened_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_priority_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_severity_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_state_node	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_pr_status_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_project	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_project_member	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_project_member_person_role	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_prsn_element_type	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_etl_history	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_failed_prs	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_grid_157	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_grid_194	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_grid_282	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_grid_338	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_grid_353	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_grid_358	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_grid_376	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_grid_380	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,	df_g_rds_grid_393	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_418	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_421	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_443	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_454	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_493	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_530	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_617	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_626	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_647	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_680	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_772	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_814	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_grid_966	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_group_type_viewable_flds	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_138	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_146	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_150	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_259	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_308	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_451	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_630	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_719	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_724	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_selection_869	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_text_331	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_multi_text_901	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_pr_1	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_ref_record_202	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_ref_record_258	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_ref_record_587	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_security_gc	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_security_va	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_security_vb	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_security_vgc	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_rds_snapshot	,"Audit_Master_ID,Audit_Source_Name")
delta_g_merge_all(Audit_Master,df_g_user_group	,"Audit_Master_ID,Audit_Source_Name")


# COMMAND ----------

# MAGIC %md
# MAGIC Staging  Audit master insert

# COMMAND ----------

Query1="""insert into {0}.gf_quality.g_audit_master values (1,'PegasusStaging','g_authorized_rep','Inc','ID','Load_date');""".format(Catalog_Name_WB)
Query2="""insert into {0}.gf_quality.g_audit_master values(2,'PegasusStaging','g_base_model','Inc','ID','Load_date');""".format(Catalog_Name_WB)
Query3="""insert into {0}.gf_quality.g_audit_master values(3,'PegasusStaging','g_ca_report_record','Full','COMPETENT_AUTHORITY','');""".format(Catalog_Name_WB)
Query4="""insert into {0}.gf_quality.g_audit_master values(4,'PegasusStaging','g_catalog_item','Inc','ID','Load_date'); """.format(Catalog_Name_WB)
Query5="""insert into {0}.gf_quality.g_audit_master values(5,'PegasusStaging','g_child_report','Full','','');""".format(Catalog_Name_WB)
Query6="""insert into {0}.gf_quality.g_audit_master values(6,'PegasusStaging','g_country_ca_mapping','Full','COUNTRY','');""".format(Catalog_Name_WB)
Query7="""insert into {0}.gf_quality.g_audit_master values(7,'PegasusStaging','g_country_mask','Full','COMPETENT_AUTHORITY, PROJECT','');""".format(Catalog_Name_WB)
Query8="""insert into {0}.gf_quality.g_audit_master values(8,'PegasusStaging','g_er_entity','Inc','CODE','Load_date');""".format(Catalog_Name_WB)
Query9="""insert into {0}.gf_quality.g_audit_master values(9,'PegasusStaging','g_event_country','Full','COUNTRY_CA','');""".format(Catalog_Name_WB)
Query10="""insert into {0}.gf_quality.g_audit_master values(10,'PegasusStaging','g_exemptions','Full','COUNTRY_CA','');""".format(Catalog_Name_WB)
Query11="""insert into {0}.gf_quality.g_audit_master values(11,'PegasusStaging','g_greedy_country','Full','COUNTRY_CA','');""".format(Catalog_Name_WB)
Query12="""insert into {0}.gf_quality.g_audit_master values(12,'PegasusStaging','g_legal_manufacturer','Inc','ID','Load_date');""".format(Catalog_Name_WB)
Query13="""insert into {0}.gf_quality.g_audit_master values(13,'PegasusStaging','g_mdm_complaint_code','Inc','CODE, VALID_FROM_DATE','Load_date');""".format(Catalog_Name_WB)
Query14="""insert into {0}.gf_quality.g_audit_master values(14,'PegasusStaging','g_mdm_country','Inc','CODE','Load_date');""".format(Catalog_Name_WB)
Query15="""insert into {0}.gf_quality.g_audit_master values(15,'PegasusStaging','g_mdm_customer','Inc','CUSTOMER_NR','Load_date');""".format(Catalog_Name_WB)
Query16="""insert into {0}.gf_quality.g_audit_master values(16,'PegasusStaging','g_mdm_owning_entity','Inc','OWNING_ENTITY_ID','Load_date ');""".format(Catalog_Name_WB)
Query17="""insert into {0}.gf_quality.g_audit_master values(20,'PegasusStaging','g_product_codes','Full','','');""".format(Catalog_Name_WB)
Query18="""insert into {0}.gf_quality.g_audit_master values(21,'PegasusStaging','g_reg_auth_applicable_countries','Inc','REGISTR_AUTHORITY','Load_date');""".format(Catalog_Name_WB)
Query19="""insert into {0}.gf_quality.g_audit_master values(22,'PegasusStaging','g_registration_data','Inc','ID, TRADEITEM_ID, TRADEITEM_ID_TYPE, REGISTR_NUMBER','Load_date');""".format(Catalog_Name_WB)
Query20="""insert into {0}.gf_quality.g_audit_master values(23,'PegasusStaging','g_reporting_class','Full','','');""".format(Catalog_Name_WB)
Query21="""insert into {0}.gf_quality.g_audit_master values(24,'PegasusStaging','g_serialized_product','Inc','EQUIPMENT_NUMBER','Load_date');""".format(Catalog_Name_WB)
Query22="""insert into {0}.gf_quality.g_audit_master values(25,'PegasusStaging','g_similar_products_mv','Full','','');""".format(Catalog_Name_WB)
Query23="""insert into {0}.gf_quality.g_audit_master values(26,'PegasusStaging','g_src_complaint_codes','Full','','');""".format(Catalog_Name_WB)
Query24="""insert into {0}.gf_quality.g_audit_master values(27,'PegasusStaging','g_src_product_hierarchy','Inc','MATNR','Load_date');""".format(Catalog_Name_WB)
Query25="""insert into {0}.gf_quality.g_audit_master values(28,'PegasusStaging','g_technical_product','Full','',''); """.format(Catalog_Name_WB)
Query26="""insert into {0}.gf_quality.g_audit_master values(29,'PegasusStaging','g_trade_item','Inc','ID,ID_TYPE','Load_date'); """.format(Catalog_Name_WB)
Query27="""insert into {0}.gf_quality.g_audit_master values(30,'PegasusStaging','g_src_catalog_profile','Full','MATNR','');""".format(Catalog_Name_WB)
Query28="""insert into {0}.gf_quality.g_audit_master values(28,'PegasusStaging','g_prd_equi_equz','Inc','EQUNR','LOAD_DATE'); """.format(Catalog_Name_WB)
Query29="""insert into {0}.gf_quality.g_audit_master values(29,'PegasusStaging','g_prd_kna1_adrc','Inc','KUNNR','LOAD_DATE'); """.format(Catalog_Name_WB)
Query30="""insert into {0}.gf_quality.g_audit_master values(30,'PegasusStaging','g_prd_mara_makt','Inc','MATNR,APPLICATION_ID','Load_date');""".format(Catalog_Name_WB)

Query31="""insert into {0}.gf_quality.g_audit_master values(31,'PegasusStaging','g_dtree_child','Full','','');""".format(Catalog_Name_WB)
Query32="""insert into {0}.gf_quality.g_audit_master values(32,'PegasusStaging','g_language_code_mapping','Full','','');""".format(Catalog_Name_WB)
Query33="""insert into {0}.gf_quality.g_audit_master values(33,'PegasusStaging','g_source_tw_country_mapping','Full','','');""".format(Catalog_Name_WB)
Query34="""insert into {0}.gf_quality.g_audit_master values(34,'PegasusStaging','g_source_tw_language_mapping','Full','','');""".format(Catalog_Name_WB)
Query35="""insert into {0}.gf_quality.g_audit_master values(35,'PegasusStaging','g_source_tw_value_mapping','Full','','');""".format(Catalog_Name_WB)
Query36="""insert into {0}.gf_quality.g_audit_master values(36,'PegasusStaging','g_src_labor_er_entity','Full','','');""".format(Catalog_Name_WB)
Query37="""insert into {0}.gf_quality.g_audit_master values(37,'PegasusStaging','g_similar_products','Full','','');""".format(Catalog_Name_WB)

# COMMAND ----------


spark.sql(Query1)
spark.sql(Query2)
spark.sql(Query3)
spark.sql(Query4)
spark.sql(Query5)
spark.sql(Query6)
spark.sql(Query7)
spark.sql(Query8)
spark.sql(Query9)
spark.sql(Query10)
spark.sql(Query11)
spark.sql(Query12)
spark.sql(Query13)
spark.sql(Query14)
spark.sql(Query15)
spark.sql(Query16)
spark.sql(Query17)
spark.sql(Query18)
spark.sql(Query19)
spark.sql(Query20)
spark.sql(Query21)
spark.sql(Query22)
spark.sql(Query23)
spark.sql(Query24)
spark.sql(Query25)
spark.sql(Query26)
spark.sql(Query27)
spark.sql(Query28)
spark.sql(Query29)
spark.sql(Query30)
spark.sql(Query31)
spark.sql(Query32)
spark.sql(Query33)
spark.sql(Query34)
spark.sql(Query35)
spark.sql(Query36)
spark.sql(Query37)


