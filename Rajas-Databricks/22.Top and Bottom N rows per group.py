# Databricks notebook source
# MAGIC %md
# MAGIC ## Find Top and Bottom N rows per Group

# COMMAND ----------

# DBTITLE 1,Create Sample Dataframe
data_student = [('Vinay', 'Physics', 80, 'P', 90),
                ('Vinay', 'Chemistry', 68, 'P', 90),
                ('Vinay', 'Mathes', 71, 'P', 90),
                ('Roopa', 'Physics', 91, 'P', 80),
                ('Roopa', 'Chemistry', 57, 'P', 80),
                ('Roopa', 'Mathes', 78, 'P', 80),
                ('Siva', 'Physics', 68, 'P', 70),
                ('Siva', 'Chemistry', 48, 'P', 70),
                ('Siva', 'Mathes', 65, 'P', 70),
                ('MMM', 'Physics', 87, 'P', 60),
                ('MMM', 'Chemistry', 56, 'P', 60),
                ('MMM', 'Mathes', 78, 'P', 60),
                ('Hony', 'Physics', 64, 'P', 75),
                ('Hony', 'Chemistry', 65, 'P', 75),
                ('Hony', 'Mathes', 98, 'P', 75)
                ]

schema = ['name', 'subject', 'mark', 'status', 'attendance']

df = spark.createDataFrame(data = data_student, schema = schema)
display(df)

# COMMAND ----------

# DBTITLE 1,Create rank wihtin each group of Name
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

windowDept = Window.partitionBy('name').orderBy(col('mark').desc())
df2 = df.withColumn('row',row_number().over(windowDept)).orderBy('name', 'row')

display(df2)

# COMMAND ----------

# DBTITLE 1,Get Top N rows per Group of Name
df3 = df2.filter(col('row') <= 1)
display(df3)

# COMMAND ----------

# DBTITLE 1,Create rank within each group of Subject
windowDept = Window.partitionBy('subject').orderBy(col('mark').desc())
df4 = df.withColumn('row',row_number().over(windowDept)).orderBy('name', 'row')

display(df4)

# COMMAND ----------

# DBTITLE 1,Get Top N rows per Group of Subject
df5 = df4.filter(col('row') <= 1)
display(df5)

# COMMAND ----------

# DBTITLE 1,Reverse Rank to get Bottom N Rows Per Group
windowDept = Window.partitionBy('subject').orderBy(col('mark'))
df6 = df.withColumn('row',row_number().over(windowDept)).orderBy('name', 'row')

display(df6)

# COMMAND ----------

# DBTITLE 1,Get Bottom N Rows Per Group
df7 = df6.filter(col('row') <= 1)
display(df7)
