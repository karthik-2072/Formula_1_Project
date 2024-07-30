# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying folder

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")

# COMMAND ----------

v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the .json folder

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/dbprojectstorage1/raw"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

qualifying_schema=StructType(fields=[StructField("qualifyId", IntegerType(), False), \
                                 StructField("raceId", IntegerType(), True), \
                                 StructField("driverId", IntegerType(),True), \
                                 StructField("constructorId", IntegerType(),True), \
                                 StructField("number",IntegerType(),True), \
                                 StructField("position",IntegerType(),True), \
                                 StructField("q1",StringType(),True), \
                                 StructField("q2",StringType(),True), \
                                StructField("q3",StringType(),True)
                                 ])

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema).option("multiLine",True).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Rename columns and add new columns
# MAGIC 1. Rename qualifyId,driverId ,constructorId and raceId
# MAGIC 2. add ingestion_date with current_timestamp()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df=qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
                      .withColumnRenamed("driverId","driver_id") \
                          .withColumnRenamed("raceId","race_id") \
                      .withColumnRenamed("constructorId","constructor_id") \
                          .withColumn("data_source",lit(v_data_source)) \
                     .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output to datalake in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))
