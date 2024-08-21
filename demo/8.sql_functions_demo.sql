-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select * CONCAT(driver_ref,'-',code) as new_driver_ref from drivers;

-- COMMAND ----------

select *, split(name,' ')[0] forename,split(name,' ')[1] surname from drivers;

-- COMMAND ----------

select *, current_timestamp from drivers

-- COMMAND ----------

select *, date_format(dob,'dd-MM-yyyy') from drivers

-- COMMAND ----------

select *, date_add(dob,1) from drivers

-- COMMAND ----------

select count(*) from drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Aggregations

-- COMMAND ----------

select max(dob) from drivers;

-- COMMAND ----------

select count(*) from drivers where nationality='British';

-- COMMAND ----------

select nationality,count(*) from drivers group by  nationality order by nationality;

-- COMMAND ----------

select nationality,count(*) from drivers group by  nationality 
having count(*)>100
order by nationality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### window functions

-- COMMAND ----------

select nationality,name,dob , RANK() over(partition by nationality order by dob desc) as age_rank from drivers order by nationality,age_rank

-- COMMAND ----------


