# Databricks notebook source
# MAGIC %md
# MAGIC ## Create sample Dataframe

# COMMAND ----------

data_student = [('Vinay', 'Science', 88, 'P', 90),
                 ('Raj', ' Maths', 75,'P', 75),
                 ('Siva', ' English', 90, 'P', 80),
                 ('Raja', ' Science',  45, 'F', 75),
                 ('Rama', ' Maths', 45, 'F', 75),
                 ('Rasul', 'Maths', 30, 'F', 50),
                 ('Kumar', ' Social', None, 'NA', 70)
                 ]

schema = ['name', 'subject', 'mark', 'status', 'attendance']

df = spark.createDataFrame(data = data_student, schema = schema)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Update the existig column

# COMMAND ----------

from pyspark.sql.functions import when

df1 = df.withColumn('status', when(df.mark >= 50, 'Pass')
                    .when(df.mark <50, 'Fail')
                    .otherwise('Absentee'))

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a new column

# COMMAND ----------

from pyspark.sql.functions import when

df2 = df.withColumn('New_Status', when(df.mark >= 50, 'Pass')
                    .when(df.mark <50, 'Fail')
                    .otherwise('Absentee'))

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Another Syntax method

# COMMAND ----------

from pyspark.sql.functions import expr

df3 = df.withColumn('new_status', expr("case when mark >= 50 THEN 'Pass' " +
                    "when mark < 50 then 'Fail' "+
                    "else 'Absentee' end"))

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-Conditions using AND and OR Operators

# COMMAND ----------

from pyspark.sql.functions import when

df4 = df.withColumn('grade', when((df.mark >= 80) & (df.attendance >= 80), 'Distinction')
                    .when((df.mark >= 50) & (df.attendance >= 50), 'Good')
                    .otherwise('Average'))

display(df4)
