# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")

# COMMAND ----------

v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

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

constructors_schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data

# COMMAND ----------

constructor_df=spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columnsand select required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_selected_df=constructor_df.select(col("constructorId").alias("constructor_id"), \
                                              col("constructorRef").alias("constructor_ref"), \
                                              col("name"), \
                                              col("nationality")
                                              )

# COMMAND ----------

display(constructor_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new Column

# COMMAND ----------

constructor_final_df=add_ingestion_date(constructor_selected_df)

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data as parquet file to datalake

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")
