# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename", StringType(), True), \
                               StructField("surname", StringType(), True)
                               ])

# COMMAND ----------

drivers_schema=StructType(fields=[StructField("driverId", IntegerType(),False), \
                                 StructField("driverRef", StringType(), True), \
                                 StructField("number", IntegerType(),True), \
                                 StructField("code",StringType(),True), \
                                 StructField("name",name_schema), \
                                 StructField("dob",DateType(),True), \
                                 StructField("nationality",StringType(),True), \
                                 StructField("url",StringType(),True)
                                 ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data

# COMMAND ----------

drivers_df=spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columnsand add required columns
# MAGIC 1. concatinate forname and surname
# MAGIC 2. ingestion_date added

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

drivers_renamed_df=drivers_df.withColumnRenamed("driverId","driver_id") \
                             .withColumnRenamed("driverRef","driver_ref") \
                             .withColumn("data_source",lit(v_data_source)) \
                             .withColumn("ingestion_date",current_timestamp()) \
                             .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

drivers_final_df=drivers_renamed_df.drop(col("url"))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data as parquet file to datalake

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/dbprojectstorage1/processed/drivers

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/drivers"))
