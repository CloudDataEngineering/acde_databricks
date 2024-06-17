# Databricks notebook source
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC use hive_metastore.f1_processed;

# COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS hive_metastore.f1_presentation.calculated_race_results
              (
                race_year int,
                team_name string,
                driver_id int,
                driver_name string,
                race_id int,
                position int,
                points int,
                calculated_points int,
                created_date TIMESTAMP,
                updated_date TIMESTAMP
              )
              using delta
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW race_results_updated
AS
SELECT 
    races.race_year,
    constructor.name AS team_name,
    driver.driver_id,
    driver.name AS driver_name,
    races.race_id,  -- Added comma here
    results.position,
    results.points,
    11 - results.position AS calculated_points
FROM
    hive_metastore.f1_processed.results 
    JOIN hive_metastore.f1_processed.driver ON results.driver_id = driver.driver_id
    JOIN hive_metastore.f1_processed.constructor ON results.constructor_id = constructor.constructor_id
    JOIN hive_metastore.f1_processed.races ON results.race_id = races.race_id 
WHERE results.position <= 10 
AND results.file_date = '{v_file_date}';
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from race_results_updated where race_year = 2021;

# COMMAND ----------

spark.sql(f"""
MERGE INTO hive_metastore.f1_presentation.calculated_race_results tar
  USING race_results_updated src
  ON (tar.driver_id = src.driver_id and tar.race_id = src.race_id)
  WHEN MATCHED THEN UPDATE SET 
    tar.position = src.position,
    tar.points = src.points,
    tar.calculated_points = src.calculated_points,
    tar.updated_date = current_timestamp()  
  WHEN NOT MATCHED THEN INSERT 
    (race_year ,team_name ,driver_id ,driver_name ,race_id ,position ,points ,calculated_points ,created_date) 
    values 
    (race_year ,team_name ,driver_id ,driver_name ,race_id ,position ,points ,calculated_points, current_timestamp());
    """)

# COMMAND ----------

# MAGIC %sql select count(*) from race_results_updated;

# COMMAND ----------

# MAGIC %sql select count(*) from hive_metastore.f1_presentation.calculated_race_results;
