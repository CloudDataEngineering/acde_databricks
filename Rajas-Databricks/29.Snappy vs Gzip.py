# Databricks notebook source
csvFile = 'dbfs:/FileStore/tables/babynames/baby_names.csv'

csvDF = (spark.read.option('sep', ',').option('inferSchema', 'true').csv(csvFile)
         )
display(csvDF)

# COMMAND ----------

csvDF.write.format('parquet').option('compression', 'snappy').save('dbfs:/FileStore/tables/babynames/snappy_parquit')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/babynames/snappy_parquit

# COMMAND ----------

csvDF.write.format('parquet').option('compression', 'gzip').save('dbfs:/FileStore/tables/babynames/gzip_parquit')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/babynames/gzip_parquit
