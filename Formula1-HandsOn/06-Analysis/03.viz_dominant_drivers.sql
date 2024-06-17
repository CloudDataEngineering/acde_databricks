-- Databricks notebook source
use hive_metastore.f1_presentation;

-- COMMAND ----------

create view v_dominant_drivers
as
select 
driver_name, 
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over (order by avg(calculated_points)desc) as driver_rk
from 
f1_presentation.calculated_race_results
group by driver_name
having count(*) >= 50
order by avg_points desc;

-- COMMAND ----------

select 
race_year,
driver_name, 
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rk <= 10)
group by race_year, driver_name
order by avg_points,race_year desc;
