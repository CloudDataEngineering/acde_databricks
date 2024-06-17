# Databricks notebook source
# MAGIC %md
# MAGIC ###Step1: Extract Tables from Azure SQL

# COMMAND ----------

# DBTITLE 1,Define JDBC Connection Parameters
jdbcHostname = 'devserver29071986.database.windows.net'
jdbcPort = 1433 
jdbcDatabase = 'test'
jdbcUsername = 'sadmin'
jdbcPassword = 'Welcome@12345'
jdbcDriver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

jdbcUrl = f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}'

# COMMAND ----------

# DBTITLE 1,Read Product Table
df_product = spark.read.format('jdbc').option('url',jdbcUrl).option('dbtable','SalesLT.Product').load()
display(df_product)

# COMMAND ----------

# DBTITLE 1,Read Sales Table
df_Sales = spark.read.format('jdbc').option('url',jdbcUrl).option('dbtable','SalesLT.SalesOrderDetail').load()
display(df_Sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step2: Transform the data as per Business rules

# COMMAND ----------

# DBTITLE 1,Cleansing Dimensioin Dataframe-Replace Null Values
df_product_cleansed = df_product.na.fill({'Size': 'M', 'Weight': 100})
display(df_product_cleansed)

# COMMAND ----------

# DBTITLE 1,Cleansing Fact Dataframe - Drop Duplicate Records
df_sales_cleansed = df_Sales.dropDuplicates()
display(df_sales_cleansed)

# COMMAND ----------

# DBTITLE 1,Join Fact and Dimension Dataframes
df_join = df_sales_cleansed.join(df_product_cleansed,df_sales_cleansed.ProductID == df_product_cleansed.ProductID, 'leftouter').select(df_sales_cleansed.ProductID, 
                                                                                                     df_sales_cleansed.UnitPrice, 
                                                                                                     df_sales_cleansed.LineTotal, 
                                                                                                     df_product_cleansed.Name,
                                                                                                     df_product_cleansed.Color, 
                                                                                                     df_product_cleansed.Size, 
                                                                                                     df_product_cleansed.Weight
                                                                                                     )

display(df_join)

# COMMAND ----------

df_agg = df_join.groupBy(['ProductID', 'Name', 'Color', 'Size', 'Weight']).sum('LineTotal').withColumnRenamed('sum(LineTotal)', 'sum_total_sales')

display(df_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step3: Load Transformed Data into Azure Data Lake Storage

# COMMAND ----------

# DBTITLE 1,Create Mount Point for ADLS Integration
dbutils.fs.mount(
    source = 'wasbs://vinayde@rajasdb.blob.core.windows.net/',
    mount_point = '/mnt/adls_test',
    extra_configs = {'fs.azure.account.key.rajasdb.blob.core.windows.net':'Account_key'}
)

# COMMAND ----------

# DBTITLE 1,List the files under mount point
dbutils.fs.ls('/mnt/adls_test')

# COMMAND ----------

# DBTITLE 1,Write the data in Parquet formate
df_agg.write.format('parquet').save('/mnt/adls_test/adv_work_parquet/')

# COMMAND ----------

# DBTITLE 1,Write the data in CSVformate
df_agg.write.format('csv').option('header', 'true').save('/mnt/adls_test/adv_work_csv/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## End of Project
