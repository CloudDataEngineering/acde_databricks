# Databricks notebook source
# DBTITLE 1,Uploaded Files List
# dbutils.fs.mkdirs('dbfs:/FileStore/tables/babynames/babynames_input')
# dbutils.fs.mkdirs('dbfs:/FileStore/tables/babynames/babynames_output')

# dbutils.fs.cp('dbfs:/FileStore/tables/babynames/baby_names.csv','dbfs:/FileStore/tables/babynames/babynames_input')

# dbutils.fs.ls('dbfs:/FileStore/tables/babynames/babynames_input')

dbutils.fs.ls('dbfs:/FileStore/tables/babynames/babynames_output')

# COMMAND ----------

# DBTITLE 1,Create Sample Dataframe
df = spark.read.format('csv').option('inferSchema', True).option('header',True).option('sep',',').load('dbfs:/FileStore/tables/babynames/babynames_input')

display(df)

# COMMAND ----------

# DBTITLE 1,Distinct Year List and Count for each Year
df.groupBy('Year').count().orderBy('Year').show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Partition By one key column
df.write.option('header',True) \
    .partitionBy('Year') \
    .mode('overwrite') \
    .csv('dbfs:/FileStore/tables/babynames/babynames_output')

# COMMAND ----------

# DBTITLE 1,Partition By Multiple Key columns
# dbutils.fs.rm('dbfs:/FileStore/tables/babynames/babynames_output', True)

dbutils.fs.mkdirs('dbfs:/FileStore/tables/babynames/babynames_output')

# COMMAND ----------

df.write.option('header',True) \
    .partitionBy('Year','Sex') \
    .mode('overwrite') \
    .csv('dbfs:/FileStore/tables/babynames/babynames_output')

# COMMAND ----------

# DBTITLE 1,Partition by key column along with maximum number of records
# dbutils.fs.rm('dbfs:/FileStore/tables/babynames/babynames_output', True)

dbutils.fs.mkdirs('dbfs:/FileStore/tables/babynames/babynames_output')

# COMMAND ----------

df.write.option('header',True) \
    .option('maxRecordsPerFile', 5000) \
    .partitionBy('Year') \
    .mode('overwrite') \
    .csv('dbfs:/FileStore/tables/babynames/babynames_output')
