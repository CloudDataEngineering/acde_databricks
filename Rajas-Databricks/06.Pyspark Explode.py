# Databricks notebook source
# MAGIC %md
# MAGIC ## Create a Dataframe wiht array column

# COMMAND ----------


array_appliance = [
    ('Vinay',['Tv','Refrigerator','Oven','AC']),
    ('Raghav',['Ac','Washing Machine', None]),
    ('Ram',['Grinder','Tv']),
    ('Roopa',['Tv','Refrigerator', None]),
    ('Rajesh', None)
]

df_app = spark.createDataFrame(data=array_appliance, schema = ['Name', 'Appliance'])
df_app.printSchema()
display(df_app)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create dataframe wiht map column

# COMMAND ----------

map_brand = [
    ('Vinay',{'Tv':'LG', 'Refrigerator':'Samsung', 'Oven':'Philips' ,'AC':'Daikin'}),
    ('Raghav',{'Ac':'Samsung', 'Washing Machine':'LG' , 'Chimni':None}),
    ('Ram',{'Grinder': 'Preethi','Tv': ''}),
    ('Roopa',{'Tv': 'Croma','Refrigerator': 'LG', 'GasStove': None}),
    ('Rajesh', None)
]

df_brand = spark.createDataFrame(data=map_brand, schema = ['Name', 'Brand'])
df_brand.printSchema()
display(df_brand)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode array field

# COMMAND ----------

from pyspark.sql.functions import explode

df2 = df_app.select(df_app.Name,explode(df_app.Appliance))

df_app.printSchema()
display(df_app)

df2.printSchema()
display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode map field

# COMMAND ----------

from pyspark.sql.functions import explode

df3 = df_brand.select(df_brand.Name,explode(df_brand.Brand))

df_brand.printSchema()
display(df_brand)

df3.printSchema()
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode outer to consider NULL values

# COMMAND ----------

from pyspark.sql.functions import explode_outer

display(df_app.select(df_app.Name, explode_outer(df_app.Appliance)))

display(df_brand.select(df_brand.Name, explode_outer(df_brand.Brand)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Positional Explode

# COMMAND ----------

from pyspark.sql.functions import posexplode

display(df_app.select(df_app.Name, posexplode(df_app.Appliance)))

display(df_brand.select(df_brand.Name, posexplode(df_brand.Brand)))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Positional Explode with NULL

# COMMAND ----------

from pyspark.sql.functions import posexplode_outer

display(df_app.select(df_app.Name, posexplode_outer(df_app.Appliance)))

display(df_brand.select(df_brand.Name, posexplode_outer(df_brand.Brand)))
