-- Databricks notebook source
use hive_metastore.f1_presentation

-- COMMAND ----------

show databases

-- COMMAND ----------

select * from f1_presentation.calculated_race_results limit 10;

-- COMMAND ----------

select 
team_name, 
count(*) total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from 
f1_presentation.calculated_race_results
group by team_name
having count(*) >= 100
order by avg_points desc;

-- COMMAND ----------

select 
team_name, 
count(*) total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from 
f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by team_name
having count(*) >= 100
order by avg_points desc;

-- COMMAND ----------

select 
team_name, 
count(*) total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from 
f1_presentation.calculated_race_results
where race_year between 2001 and 2010
group by team_name
having count(*) >= 50
order by avg_points desc;
