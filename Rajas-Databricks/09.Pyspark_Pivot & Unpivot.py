# Databricks notebook source
# MAGIC %md
# MAGIC ## Create a sample Dataframe

# COMMAND ----------

data =[('ABC', 'Q1', 2000),
       ('ABC', 'Q2', 3000),
       ('ABC', 'Q3', 4000),
       ('ABC', 'Q4', 6000),
       ('XYZ', 'Q1', 5000),
       ('XYZ', 'Q2', 7000),
       ('XYZ', 'Q3', 8000),
       ('XYZ', 'Q4', 9000),
       ('MMM', 'Q1', 4000),
       ('MMM', 'Q2', 5000),
       ('MMM', 'Q3', 7000),
       ('MMM', 'Q4', 8000),
       ]

column = ['Company', 'Quarter', 'Revenue']

df = spark.createDataFrame(data = data, schema = column)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pivot a DataFrame

# COMMAND ----------

Pivot_DF = df.groupBy('Company').pivot('Quarter').sum('Revenue')

display(Pivot_DF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unpivot a Dataframe

# COMMAND ----------

df1 = Pivot_DF.selectExpr("Company", "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (Quarter, Revenue)")

display(df1)
