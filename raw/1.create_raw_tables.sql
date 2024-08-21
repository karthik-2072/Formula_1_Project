-- Databricks notebook source
create database if not exists f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### create circuits table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/mnt/dbprojectstorage1/raw"))

-- COMMAND ----------

create table if not exists f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/dbprojectstorage1/raw/circuits.csv")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
create table if not exists f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/dbprojectstorage1/raw/races.csv")

-- COMMAND ----------

select * from f1_raw.races;
