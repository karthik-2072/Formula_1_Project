# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap_time folder

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")

# COMMAND ----------

v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the .json file

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dbprojectstorage1/raw"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

lap_times_schema=StructType(fields=[StructField("raceId", IntegerType(), False), \
                                 StructField("driverId", IntegerType(),True), \
                                 StructField("lap",IntegerType(),True), \
                                 StructField("position",IntegerType(),True), \
                                 StructField("time",StringType(),True), \
                                 StructField("milliseconds",IntegerType(),True)
                                 ])

# COMMAND ----------

lap_times_df=spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. add ingestion_date with current_timestamp()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df=lap_times_df.withColumnRenamed("driverId","driver_id") \
                     .withColumnRenamed("raceId","race_id") \
                        .withColumn("data_source",lit(v_data_source)) \
                     .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output to datalake in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))
