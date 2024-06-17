# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select current_database();
# MAGIC use hive_metastore.default;

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists hive_metastore.f1_demo
# MAGIC location '/mnt/adlsacde/demo';

# COMMAND ----------

# display(spark.read.option('inferSchema', True).json('/mnt/adlsacde/raw/2021-03-28/results.json').limit(10))
results_df = spark.read.option('inferSchema', True).json('/mnt/adlsacde/raw/2021-03-28/results.json')
# display(results_df.limit(10))

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').saveAsTable('hive_metastore.f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.results_managed limit 10;

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').save('/mnt/adlsacde/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists hive_metastore.f1_demo.results_external
# MAGIC using delta
# MAGIC location '/mnt/adlsacde/demo/results_external';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.results_external limit 10;

# COMMAND ----------

results_external_df = spark.read.format('delta').load('/mnt/adlsacde/demo/results_external')
display(results_external_df.limit(10))

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').partitionBy('constructorId').saveAsTable('hive_metastore.f1_demo.results_partitioned')

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions hive_metastore.f1_demo.results_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete From Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC update hive_metastore.f1_demo.results_managed
# MAGIC set points = 11 - position
# MAGIC where points <= 10;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/adlsacde/demo/results_managed")
deltaTable.update(
    condition='position <= 10',
    set={'points': '21 - position'}
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM hive_metastore.f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/adlsacde/demo/results_managed")
deltaTable.delete(condition='points is null')

# COMMAND ----------

deltaTable.toDF().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Upsert using merge

# COMMAND ----------

driver_day1_df = spark.read.option("inferSchema", "true").\
                json('/mnt/adlsacde/raw/2021-03-28/drivers.json').\
                filter('driverId <= 10').\
                select('driverId', 'dob', 'name.forename', 'name.surname')

display(driver_day1_df.limit(5))

# COMMAND ----------

driver_day1_df.createOrReplaceTempView('driver_day1_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from driver_day1_df;

# COMMAND ----------

from pyspark.sql.functions import upper

driver_day2_df = spark.read.option("inferSchema", "true").\
                json('/mnt/adlsacde/raw/2021-03-28/drivers.json').\
                filter('driverId between 6 and 15').\
                select('driverId', 'dob', upper('name.forename').alias('forename'), upper('name.surname').alias('surname'))
display(driver_day2_df.limit(10))

# COMMAND ----------

driver_day2_df.createOrReplaceTempView('driver_day2_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from driver_day2_df;

# COMMAND ----------

driver_day3_df = spark.read.option('inferSchema', True).\
                json('/mnt/adlsacde/raw/2021-03-28/drivers.json')\
                .filter('driverId between 1 and 5 or driverId between 16 and 20')\
                .select('driverId', 'dob', upper('name.forename').alias('forename'), upper('name.surname').alias('surname') )
display(driver_day3_df.count())

# COMMAND ----------

driver_day3_df.createOrReplaceTempView('driver_day3_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from driver_day3_df;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists hive_metastore.f1_demo.drivers_merge(
# MAGIC driverId int,
# MAGIC dob Date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC created_date date,
# MAGIC updated_date date
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day-1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hive_metastore.f1_demo.drivers_merge tar
# MAGIC   USING driver_day1_df src
# MAGIC   ON tar.driverId = src.driverId
# MAGIC   WHEN MATCHED THEN UPDATE SET 
# MAGIC     tar.dob = src.dob,
# MAGIC     tar.forename = src.forename,
# MAGIC     tar.surname = src.surname,
# MAGIC     tar.updated_date = current_timestamp()  
# MAGIC   WHEN NOT MATCHED THEN INSERT 
# MAGIC     (driverId, dob, forename, surname, created_date) values 
# MAGIC     (src.driverId, src.dob, src.forename, src.surname, current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day-2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hive_metastore.f1_demo.drivers_merge tar
# MAGIC   USING driver_day2_df src
# MAGIC   ON tar.driverId = src.driverId
# MAGIC   WHEN MATCHED THEN UPDATE SET 
# MAGIC     tar.dob = src.dob,
# MAGIC     tar.forename = src.forename,
# MAGIC     tar.surname = src.surname,
# MAGIC     tar.updated_date = current_timestamp()  
# MAGIC   WHEN NOT MATCHED THEN INSERT 
# MAGIC     (driverId, dob, forename, surname, created_date) values 
# MAGIC     (src.driverId, src.dob, src.forename, src.surname, current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day-3

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/adlsacde/demo/drivers_merge')

deltaTable.alias('trg').merge(
    driver_day3_df.alias('src'),
    'trg.driverId = src.driverId')\
        .whenMatchedUpdate(set = {'dob' : 'src.dob', 'forename' : 'src.forename', 'surname' : 'src.surname', 'updated_date' : 'current_timestamp()'}) \
        .whenNotMatchedInsert(values=
                              {
                                  'driverId' : 'src.driverId',
                                  'dob' : 'src.dob',
                                  'forename' : 'src.forename',
                                  'surname' : 'src.surname',
                                  'created_date' : 'current_timestamp()'
                              }) \
        .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge order by driverId;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC history hive_metastore.f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge version as of 4 limit 3;

# COMMAND ----------

df_version = spark.read.format('delta').option('versionAsOf','4').load('/mnt/adlsacde/demo/drivers_merge')
display(df_version.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge timestamp as of '2024-06-04T05:02:26.000+00:00';

# COMMAND ----------

df_timestamp = spark.read.format('delta').option('timestampAsOf','2024-06-04T05:02:26.000+00:00').load('/mnt/adlsacde/demo/drivers_merge')
display(df_timestamp.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM hive_metastore.f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge timestamp as of '2024-06-04T05:02:26.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM hive_metastore.f1_demo.drivers_merge RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history hive_metastore.f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge timestamp as of '2024-06-04T05:02:26.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge order by driverId limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from hive_metastore.f1_demo.drivers_merge where driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history hive_metastore.f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge version as of 12 order by driverId;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into hive_metastore.f1_demo.drivers_merge as trg
# MAGIC   using hive_metastore.f1_demo.drivers_merge version as of 12 as src
# MAGIC   on (trg.driverId = src.driverId)
# MAGIC   when not matched then 
# MAGIC     Insert * ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.f1_demo.drivers_merge order by driverId;

# COMMAND ----------

# MAGIC %sql desc history hive_metastore.f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.f1_demo.drivers_txn (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history hive_metastore.f1_demo.drivers_txn; 

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into hive_metastore.f1_demo.drivers_txn
# MAGIC select * from hive_metastore.f1_demo.drivers_merge where driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from hive_metastore.f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from hive_metastore.f1_demo.drivers_txn where driverId = 1;

# COMMAND ----------

for driverId in range(3,20):
    spark.sql(f"""insert into hive_metastore.f1_demo.drivers_txn
              select * from hive_metastore.f1_demo.drivers_merge
              where driverId = {driverId}""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Convert ParquetToDelta

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists hive_metastore.f1_demo.drivers_convert_to_delta(
# MAGIC driverId int,
# MAGIC dob Date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC created_date date,
# MAGIC updated_date date
# MAGIC )
# MAGIC using parquet;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into hive_metastore.f1_demo.drivers_convert_to_delta
# MAGIC select * from hive_metastore.f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta hive_metastore.f1_demo.drivers_convert_to_delta;

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/adlsacde/demo/drivers_convert_to_delta/')
display(df.limit(10))

# COMMAND ----------

df.write.format('parquet').save('/mnt/adlsacde/demo/drivers_convert_to_delta_new')

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/adlsacde/demo/drivers_convert_to_delta_new`;
