-- Databricks notebook source
show databases

-- COMMAND ----------

select current_database();

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from f1_processed.drivers;

-- COMMAND ----------

select * from drivers where nationality='British' and dob>='1990-01-01';

-- COMMAND ----------

select name,dob from drivers where nationality='British' and dob>='1990-01-01';

-- COMMAND ----------

select name,dob as Date of Birth from drivers where nationality='British' and dob>='1990-01-01';

-- COMMAND ----------

select name,dob as Date of Birth from drivers where nationality='British' and dob>='1990-01-01' order by rank asc;

-- COMMAND ----------

select * from drivers order by nationality asc,
                               dob desc;

-- COMMAND ----------


