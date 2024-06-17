-- Databricks notebook source
use hive_metastore.f1_presentation;

-- COMMAND ----------

create view v_dominant_team
as
select 
team_name, 
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over (order by avg(calculated_points)desc) as team_rk
from 
f1_presentation.calculated_race_results
group by team_name
having count(*) >= 50
order by avg_points desc;

-- COMMAND ----------

select 
race_year,
team_name, 
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_team where team_rk <= 10)
group by race_year, team_name
order by avg_points,race_year desc;
