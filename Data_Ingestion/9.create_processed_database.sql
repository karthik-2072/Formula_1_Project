-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/dbprojectstorage1/processed"

-- COMMAND ----------

desc database f1_processed

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/dbprojectstorage1/presentation"

-- COMMAND ----------

desc database f1_presentation
