# Databricks notebook source
from delta.tables import *
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date, timedelta, datetime

# COMMAND ----------

#set merge on clause
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

def delta_merge(TargetTable,df_Source,JoinCondtion):
	
    join_Condition=Set_On_Merge_Clause(JoinCondtion)
    Update_Column = df_Source.columns
    Update_Column.remove("L2_LOADED_DATE")

    update_set = {}
    for column in Update_Column:
        update_set[f"{column}"]= f"s.{column}"

    Target_delta = DeltaTable.forName(spark,f"{TargetTable}")
    Target_delta.alias("t").merge(df_Source.alias("s"),join_Condition ).\
        whenMatchedUpdate(set=update_set).whenNotMatchedInsertAll().execute()


# COMMAND ----------

def delta_merge_all(TargetTable,df,JoinCondtion):
	
    join_Condition=Set_On_Merge_Clause(JoinCondtion)
    columns= df.columns
    update_set ={}
    for column in columns:
	    update_set[f"{column}"]=f"s.{column}"
	 
    Target_delta = DeltaTable.forName(spark,f"{TargetTable}")
    #return update_set
    Target_delta.alias("t").merge(df.alias("s"),join_Condition ).whenMatchedUpdate(set=update_set).whenNotMatchedInsertAll().execute()


# COMMAND ----------

def Delta_OverWrite(TargetTable,df):
	
    #return update_set
    df.write.format("delta").mode("overwrite").saveAsTable(TargetTable)


# COMMAND ----------

def Delta_Append(TargetTable,df):
    df.write.format("delta").mode("append").saveAsTable(TargetTable)
