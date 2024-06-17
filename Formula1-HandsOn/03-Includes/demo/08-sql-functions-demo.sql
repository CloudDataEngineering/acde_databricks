-- Databricks notebook source
use hive_metastore.f1_processed;

-- COMMAND ----------

select * 
from 
f1_processed.driver limit 10;

-- COMMAND ----------

select *, 
concat(driver_ref, '_', code) as new_driver_ref,
split(name, ' ') as driver_name,
split(name, ' ') [0] as driver_firstname,
split(name, ' ') [1] as driver_lastname,
current_timestamp() as time_stamp,
date_format(dob, 'dd-MM-yyyy') as date_of_birth,
date_add(dob,1) as dob_1
from 
f1_processed.driver limit 10;

-- COMMAND ----------

select count(*) as count,nationality
from f1_processed.driver
group by nationality
order by count desc;

-- COMMAND ----------

select count(*) as count,nationality
from f1_processed.driver
group by nationality
having count(*) > (100)
order by count desc;

-- COMMAND ----------

select 
nationality , name, dob,
rank() over (partition by nationality order by dob desc) as age_rk
from 
f1_processed.driver
order by nationality, age_rk;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SQL Joins

-- COMMAND ----------

use f1_presentation

-- COMMAND ----------

drop view if exists v_driver_standing_2018;

create view if not exists v_driver_standing_2018
as
select 
race_year,driver_name,team_name,Total_points,Wins,Rank 
from 
f1_presentation.driver_standing 
where race_year = 2018;

select * from v_driver_standing_2018 limit 10;

-- COMMAND ----------

drop view if exists v_driver_standing_2020;

create view if not exists v_driver_standing_2020
as
select 
race_year,driver_name,team_name,Total_points,Wins,Rank 
from 
f1_presentation.driver_standing 
where race_year = 2020;

select * from v_driver_standing_2020 limit 10;

-- COMMAND ----------

-- DBTITLE 1,Driver Standing Comparison Using Join
select *
from
v_driver_standing_2018 d_2018
join v_driver_standing_2020 d_2020
on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- DBTITLE 1,Driver Standing Comparison using inner join
select * from 
v_driver_standing_2018 d_2018
inner join v_driver_standing_2020 d_2020
on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- DBTITLE 1,Driver Standing Comparison Using left join
select * from 
v_driver_standing_2018 d_2018
left join v_driver_standing_2020 d_2020
on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- DBTITLE 1,Driver Standing Comparison Using Right Jion
select * from 
v_driver_standing_2018 d_2018
right join v_driver_standing_2020 d_2020
on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- DBTITLE 1,Driver Standing Comparison Using full join
select * from 
v_driver_standing_2018 d_2018
full join v_driver_standing_2020 d_2020
on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- DBTITLE 1,Driver Standing Comparison semi join
select * from 
v_driver_standing_2018 d_2018
semi join v_driver_standing_2020 d_2020
on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- DBTITLE 1,Driver Standing Comparison Using anti join
select * from 
v_driver_standing_2018 d_2018
anti join v_driver_standing_2020 d_2020
on d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

select * from 
v_driver_standing_2018 d_2018
cross join v_driver_standing_2020 d_2020;
