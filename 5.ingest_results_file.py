# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the json file using spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dbprojectstorage1/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema

# COMMAND ----------

results_schema="resultId INT,raceId INT,driverId INT,constructorId INT,number INT,grid INT,position INT,positionText STRING,positionOrder INT,points FLOAT,laps INT,time STRING,milliseconds INT,fastestLap INT,rank INT,fastestLapTime STRING,fastestLapSpeed FLOAT,statusId STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data

# COMMAND ----------

results_df=spark.read \
.schema(results_schema) \
.json("dbfs:/mnt/dbprojectstorage1/raw/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columnsand select required columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_renamed_df=results_df.withColumnRenamed("resultId","result_id") \
                             .withColumnRenamed("raceId","race_id") \
                             .withColumnRenamed("driverId","driver_id") \
                             .withColumnRenamed("ConstructorId","constructor_id") \
                              .withColumnRenamed("positionText","position_text") \
                              .withColumnRenamed("positionOrder","position_order") \
                              .withColumnRenamed("fastestLap","fastest_lap") \
                              .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                               .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                             .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df=results_renamed_df.drop(col("statusId"))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data as parquet file to datalake

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/dbprojectstorage1/processed/results")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/dbprojectstorage1/processed/results

# COMMAND ----------

display(spark.read.parquet("/mnt/dbprojectstorage1/processed/results"))