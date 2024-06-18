# Databricks notebook source
# DBTITLE 1,# import libraries
from delta.tables import *
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date, timedelta, datetime

# COMMAND ----------

# DBTITLE 1,Generic Function to create clause for merge
def Set_On_Merge_Clause(JoinCondtion):
  JoinCondtion=JoinCondtion.split(',')

  for i in range(len(JoinCondtion)):
    #for i in range(0,len(JoinCondtion)):
        Key=JoinCondtion[i]
        if i==0:
            on_clause="  t.{Key} == s.{Key} ".format(Key=Key)
        else:
            on_clause2=" and t.{Key} == s.{Key} ".format(Key=Key)
            on_clause=on_clause+on_clause2
            
  return on_clause

# COMMAND ----------

# DBTITLE 1,Generic Function to merage data by removing G_load_date( crated_date in gold table)
def delta_g_merge(TargetTable,df_Source,JoinCondtion):
    join_Condition=Set_On_Merge_Clause(JoinCondtion)
    Update_Column = df_Source.columns
    Update_Column.remove("G_LOADED_DATE")
    update_set = {}
    for column in Update_Column:
        update_set[f"{column}"]= f"s.{column}"

    Target_delta = DeltaTable.forName(spark,f"{TargetTable}")
    Target_delta.alias("t").merge(df_Source.alias("s"),join_Condition ).\
        whenMatchedUpdate(set=update_set).whenNotMatchedInsertAll().execute()


# COMMAND ----------

# DBTITLE 1,Generic Function to merage data for all column
def delta_g_merge_all(TargetTable,df,JoinCondtion):

    join_Condition=Set_On_Merge_Clause(JoinCondtion)
    columns= df.columns
    update_set ={}
    for column in columns:
        update_set[f"{column}"]=f"s.{column}"
    Target_delta = DeltaTable.forName(spark,f"{TargetTable}")
        #return update_set
    Target_delta.alias("t").merge(df.alias("s"),join_Condition ).whenMatchedUpdate(set=update_set).whenNotMatchedInsertAll().execute()


# COMMAND ----------

# DBTITLE 1,Generic Function to overwrite data
def Delta_g_OverWrite(TargetTable,df):
	
    #return update_set option("overwriteSchema", "true")'
    df.write.format("delta").mode("overwrite").option("mergeSchema" ,"true").saveAsTable(TargetTable)


# COMMAND ----------

# DBTITLE 1,Generic Function to append data
def Delta_g_Append(TargetTable,df):
    df.write.format("delta").mode("append").saveAsTable(TargetTable)
