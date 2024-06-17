# Databricks notebook source
data = [('vinay pvt ltd', 'Q1', 2500),
        ('Roopa pvt ltd', 'Q1', 5000),
        ('MMM pvt ltd', 'Q1', 7500)
        ]

column = ['company', 'Quarter', 'Revenue']

df = spark.createDataFrame(data = data, schema = column)
display(df)

# COMMAND ----------

df.write.saveAsTable('default.fact_revenue')

# COMMAND ----------

display(spark.table('default.fact_revenue'))

# COMMAND ----------

display(spark.sql('select * from default.fact_revenue'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.fact_revenue

# COMMAND ----------

data = [('Siva pvt ltd', 'Q4', 9000)]

column = ['company', 'Quarter', 'Revenue']

df1 = spark.createDataFrame(data = data, schema = column)
display(df1)

# COMMAND ----------

df1.write.insertInto('default.fact_revenue', overwrite= False)

# COMMAND ----------

display(spark.table('default.fact_revenue'))

# COMMAND ----------

data = [('Hony pvt ltd', 'Q3', 10000)]

column = ['company', 'Quarter', 'Revenue']

df2 = spark.createDataFrame(data = data, schema = column)
display(df2)

# COMMAND ----------

df2.write.insertInto('default.fact_revenue', overwrite= True)

# COMMAND ----------

display(spark.table('default.fact_revenue'))

# COMMAND ----------

data = [('ABC pvt ltd', 'Q3', 10000)]

column = ['company', 'Quarter', 'Revenue']

df3 = spark.createDataFrame(data = data, schema = column)
display(df3)

# COMMAND ----------

df3.createOrReplaceTempView('v_insert_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into default.fact_revenue
# MAGIC select * from v_insert_data

# COMMAND ----------

display(spark.table('default.fact_revenue'))
