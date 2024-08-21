-- Databricks notebook source
create or replace temp view v_dominant_teams
as
select team_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points,
       rank() over (order by avg(calculated_points)desc) team_rank
 from f1_presentation.calculated_race_results
 group by team_name
 having count(1)>=100
 order by avg_points desc

-- COMMAND ----------

select race_year,
       team_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points,
 from f1_presentation.calculated_race_results
 where driver_name in (select driver_name from v_dominant_teams where driver_rank<=10)
 group by race_year,team_name
 order by race_year,avg_points desc
