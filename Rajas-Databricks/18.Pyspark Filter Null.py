# Databricks notebook source
# MAGIC %md
# MAGIC ## Create sample Dataframe

# COMMAND ----------

data_student = [('Vinay', 'Science', 88, 'P', 90),
                 ('Raj', ' Maths', 75,'P', None),
                 ('Siva', ' English', 90, 'P', 80),
                 ('Raja', ' Science',  45, 'F', 75),
                 ('Rama', ' Maths', None, 'F', None),
                 ('Rasul', None, 30, 'F', 50),
                 ('Kumar', ' Social', None, None, 70)
                 ]

schema = ['name', 'subject', 'mark', 'status', 'attendance']

df = spark.createDataFrame(data = data_student, schema = schema)
display(df)


# COMMAND ----------

# DBTITLE 1,Filter All records with null values
# display(df.filter(df.mark.isNull()))

# display(df.filter('mark IS NULL'))

from pyspark.sql.functions import col
display(df.filter(col('mark').isNull()))

# COMMAND ----------

# DBTITLE 1,Filter All records without null values
# display(df.filter(df.mark.isNotNull()))

# display(df.filter((df.mark.isNotNull()) & (df.attendance.isNotNull())))
display(df.filter((df.mark.isNotNull()) | (df.attendance.isNotNull())))

# display(df.filter('mark IS Not NULL'))

# from pyspark.sql.functions import col
# display(df.filter(col('mark').isNotNull()))

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
