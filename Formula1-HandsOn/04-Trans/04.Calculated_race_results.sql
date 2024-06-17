-- Databricks notebook source
use hive_metastore.f1_processed;

-- COMMAND ----------

show tables;

-- COMMAND ----------

show databases

-- COMMAND ----------

DROP TABLE IF EXISTS hive_metastore.f1_presentation.calculated_race_results;

CREATE TABLE IF NOT EXISTS hive_metastore.f1_presentation.calculated_race_results
USING parquet
AS
SELECT 
    races.race_year,
    constructor.name AS team_name,
    driver.name AS driver_name,
    results.position,
    results.points,
    11 - results.position AS calculated_points
FROM
    results 
JOIN driver ON results.driver_id = driver.driver_id
JOIN constructor ON results.constructor_id = constructor.constructor_id
JOIN races ON results.race_id = races.race_id 
WHERE results.position <= 10 
ORDER BY races.race_year;

-- COMMAND ----------

select * from f1_presentation.calculated_race_results limit 10;
