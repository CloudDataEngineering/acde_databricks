# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType,LongType

# COMMAND ----------

# MAGIC %md ####Audit Master Table

# COMMAND ----------

# MAGIC %run /Shared/release/HayStack/Common_Functions/Haystack_Merge

# COMMAND ----------

# MAGIC %md #####Audit Master Data Entry - QDS

# COMMAND ----------

Audit_Master=Catalog_Name_L2+'.qnr.l2_audit_master'

schema_master = StructType([
    StructField("Audit_Master_ID",IntegerType(),True),
    StructField("Audit_Source_Name",StringType(),True),
    StructField("Audit_Table_Name",StringType(),True),
    StructField("Audit_Merge_Key",StringType(),True)
    ])

# COMMAND ----------

# df_Complaint = spark.createDataFrame([[1,"QDS","COMPLAINT",'COMPLAINT_ID,APPLICATION_ID' ] ],schema_master )
# delta_merge_all(Audit_Master,df_Complaint,"Audit_Master_ID,Audit_Source_Name" )
# df_Complaint_F = spark.createDataFrame([[2,"QDS","COMPLAINT_FAILURE",'COMPLAINT_ID,SEQ_NO,APPLICATION_ID' ] ],schema_master )
# delta_merge_all(Audit_Master,df_Complaint_F,"Audit_Master_ID,Audit_Source_Name" )
# df_Complaint_H = spark.createDataFrame([[3,"QDS","COMPLAINT_HAZARD",'COMPLAINT_ID,SEQ_NO,APPLICATION_ID' ] ],schema_master )
# delta_merge_all(Audit_Master,df_Complaint_H,"Audit_Master_ID,Audit_Source_Name" )
# df_Complaint_PC = spark.createDataFrame([[4,"QDS","COMPLAINT_PATIENT_OUTCOME",'COMPLAINT_ID,SEQ_NO,APPLICATION_ID' ] ],schema_master )
# delta_merge_all(Audit_Master,df_Complaint_PC,"Audit_Master_ID,Audit_Source_Name" )

# df_Complaint_PC = spark.createDataFrame([[5,"QDS","COMPLAINT_PART_USED",'COMPLAINT_ID,SEQ_NO,APPLICATION_ID' ] ],schema_master )
# delta_merge_all(Audit_Master,df_Complaint_PC,"Audit_Master_ID,Audit_Source_Name" )
# df_Complaint_PC = spark.createDataFrame([[6,"QDS","COMPLAINT_DUPLICATE",'COMPLAINT_ID,COMPLAINT_ID_DUPLICATE,APPLICATION_ID,APPLICATION_ID_DUPLICATE' ] ],schema_master )
# delta_merge_all(Audit_Master,df_Complaint_PC,"Audit_Master_ID,Audit_Source_Name" )
# df_Complaint_PC = spark.createDataFrame([[7,"QDS","COMPLAINT_EVALUATION_RESULT",'COMPLAINT_ID,SEQ_NO,APPLICATION_ID' ] ],schema_master )
# delta_merge_all(Audit_Master,df_Complaint_PC,"Audit_Master_ID,Audit_Source_Name" )
# df_Complaint_PC = spark.createDataFrame([[8,"QDS","COMPLAINT_COMPONENT",'COMPLAINT_ID,SEQ_NO,APPLICATION_ID' ] ],schema_master )
# delta_merge_all(Audit_Master,df_Complaint_PC,"Audit_Master_ID,Audit_Source_Name" )

# COMMAND ----------

df_Complaint = spark.createDataFrame([[9,"CatsWeb","COMPLAINT",'COMPLAINT_ID,APPLICATION_ID' ] ],schema_master )
delta_merge_all(Audit_Master,df_Complaint,"Audit_Master_ID,Audit_Source_Name" )
df_Complaint_H = spark.createDataFrame([[10,"CatsWeb","COMPLAINT_HAZARD_CATSWEB",'COMPLAINT_ID' ] ],schema_master )
delta_merge_all(Audit_Master,df_Complaint_H,"Audit_Master_ID,Audit_Source_Name" )
df_Complaint_PC = spark.createDataFrame([[11,"CatsWeb","COMPLAINT_PATIENT_OUTCOME_CATSWEB",'COMPLAINT_ID' ] ],schema_master )
delta_merge_all(Audit_Master,df_Complaint_PC,"Audit_Master_ID,Audit_Source_Name" )

# COMMAND ----------

spark.sql("select * from {}.qnr.l2_audit_master".format(Catalog_Name_L2)).show()

# COMMAND ----------


