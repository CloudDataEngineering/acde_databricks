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
                 ('Kumar', ' Social', None, None, 70),
                 (None, None, None, None, None)
                 ]

schema = ['name', 'subject', 'mark', 'status', 'attendance']

df = spark.createDataFrame(data = data_student, schema = schema)
display(df)


# COMMAND ----------

# DBTITLE 1,Drop the records with Null Value - ALL & ANY
# display(df.na.drop())

display(df.na.drop('all'))

# display(df.dropna('any'))

# COMMAND ----------

# DBTITLE 1,Drop the records with Null Value on selected column
# display(df.na.drop(subset=['mark'])) # it could be combination of Columns

display(df.na.drop(subset=['mark','attendance'])) # it could be combination of Columns

# COMMAND ----------

# DBTITLE 1,Fill Value for all columns if Null is Present
# display(df.na.fill(value=0))

# display(df.na.fill(value='NA'))

display(df.fillna(value=0))

# COMMAND ----------

# DBTITLE 1,Fill value for specific columns if contains null
# display(df.na.fill(value=0, subset=['mark', 'attendance']))

display(df.na.fill({'mark': 0, 'subject': 'NA', 'Name': 'No-Name','subject':'English', 'attendance': '50'}))
