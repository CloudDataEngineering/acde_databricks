# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv File

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../03-Includes/01-Configuration"

# COMMAND ----------

# MAGIC %run "../03-Includes/02-common_functions"

# COMMAND ----------

# DBTITLE 1,Display ADLS Raw Files
display(dbutils.fs.mounts())
# display(dbutils.fs.ls(raw_folder_path))

# COMMAND ----------

# import pandas as pd
# from pyspark.sql import SparkSession

# # Create SparkSession
# spark = SparkSession.builder.getOrCreate()

# # Read the pandas DataFrame
# df_circuits = pd.read_csv('https://raw.githubusercontent.com/CloudDataEngineering/acde_databricks/main/raw/files/formula1/circuits.csv')

# # Convert pandas DataFrame to Spark DataFrame
# spark_df_circuits = spark.createDataFrame(df_circuits)

# # Write Spark DataFrame to CSV
# spark_df_circuits.write.mode('overwrite').option('header', 'true').option('inferSchema', 'true').format('csv').save(f'{raw_folder_path}/circuits.csv')

# # Read the saved CSV file as a Spark DataFrame
# spark_df = spark.read.csv(f'{raw_folder_path}/circuits.csv', header=True)

# # Display the Spark DataFrame
# # display(spark_df).limit(10)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Read and display circuit data.
# display(spark.read.csv('dbfs:/mnt/adlsacde/raw/circuits.csv',header = True))
# circuits_df = spark.read.csv('dbfs:/mnt/adlsacde/raw/circuits.csv',header = True)

# circuits_df = spark.read. \
#     option("header", True). \
#     option('inferSchema', True). \
#     csv('dbfs:/mnt/adlsacde/raw/circuits.csv')

circuits_df = spark.read. \
    option("header", True). \
    schema(circuits_schema). \
    csv(f'{raw_folder_path}/circuits.csv')
    # csv('/mnt/adlsacde/raw/circuits.csv')

display(circuits_df.limit(10))

# COMMAND ----------

# display(dbutils.fs.mounts())
# display(dbutils.fs.ls('/mnt/adlsacde/raw'))
# display(dbutils.fs.ls(f'{raw_folder_path}/lap_times'))

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

circuits_select_df1 = circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')
display(circuits_select_df1.limit(10))

# COMMAND ----------

circuits_select_df2 = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt )

display(circuits_select_df2.limit(10))

# COMMAND ----------

circuits_select_df3 = circuits_df.select(circuits_df['circuitId'], circuits_df['circuitRef'], circuits_df['name'], circuits_df['location'], circuits_df['country'], circuits_df['lat'], circuits_df['lng'], circuits_df['alt'] )

display(circuits_select_df3.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col

circuits_select_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt') )

display(circuits_select_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit,current_timestamp

circuits_renamed_df = circuits_select_df.withColumnRenamed('circuitId', 'circuit_id'). \
                                         withColumnRenamed('circuitRef', 'circuit_ref'). \
                                         withColumnRenamed('lat', 'latitude'). \
                                         withColumnRenamed('lng', 'longitude'). \
                                         withColumnRenamed('alt', 'altitude'). \
                                         withColumn('data_source', lit('dbdemostore.circuits')). \
                                         withColumn('env', lit('Devlopment'))

display(circuits_renamed_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add Ingection date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
                                        
display(circuits_final_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquit 

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# circuits_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/circuits')
circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable('hive_metastore.f1_processed.circuits')

# COMMAND ----------

dbutils.fs.ls(processed_folder_path)

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/circuits/').limit(10))
