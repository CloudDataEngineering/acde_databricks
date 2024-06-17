# Databricks notebook source
# DBTITLE 1,Create Dataframe
sampleData = (('Vinay Kumar M', 'Sales', 3000),\
            ('Siva Kumar M', 'Sales', 4600),\
                ('Roopa M', 'Sales', 4100),\
                    ('Kumar M', 'Sales', 3000),\
                        ('Vinay M', 'Sales', 4100),\
                            ('MMM Kumar M', 'Finance', 3000),\
                                ('Hony Kumar M', 'Finance', 3300),\
                                    ('Subbu Kumar M', 'Finance', 3900),\
                                        ('Sujana M', 'Marketing', 3000),\
                                            ('ganesh', 'Marketing', 2000)
                                                )

columns = ['employee_name','department', 'salary']

df = spark.createDataFrame(data = sampleData, schema = columns)

df.show()

# COMMAND ----------

# DBTITLE 1,Window Definition
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy('department').orderBy('salary')

# COMMAND ----------

# DBTITLE 1,Lag Window Function
from pyspark.sql.functions import lag
df.withColumn('lag',lag('salary',1).over(windowSpec)) \
    .show()

# COMMAND ----------

# DBTITLE 1,Lead Window Function
from pyspark.sql.functions import lead
df.withColumn('lead',lead('salary',1).over(windowSpec)) \
    .show()
