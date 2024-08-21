-- Databricks notebook source
use f1_processed

-- COMMAND ----------

desc driver_standings

-- COMMAND ----------

create or replace temp view v_driver_standings_2018
as 
select race_year,driver_name,team.total_points,wins,rank
from driver_standings
where race_year=2018;

-- COMMAND ----------

select * from v_driver_standings_2018

-- COMMAND ----------

create or replace temp view v_driver_standings_2020
as 
select race_year,driver_name,team.total_points,wins,rank
from driver_standings
where race_year=2020;

-- COMMAND ----------

select * from v_driver_standings_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inner join

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
join v_driver_standings_2018 d_2020
on(d_2018.driver_name=d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Left join

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
left join v_driver_standings_2018 d_2020
on(d_2018.driver_name=d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Right join

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
right join v_driver_standings_2018 d_2020
on(d_2018.driver_name=d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### full join

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
full join v_driver_standings_2018 d_2020
on(d_2018.driver_name=d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Semi join

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
semi join v_driver_standings_2018 d_2020
on(d_2018.driver_name=d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### anti join

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
anti join v_driver_standings_2018 d_2020
on(d_2018.driver_name=d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### cross join

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
cross join v_driver_standings_2018 d_2020
