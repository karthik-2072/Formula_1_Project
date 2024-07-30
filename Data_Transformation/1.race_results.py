# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number","driver_number") \
    .withColumnRenamed("name","driver_name") \
    .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name","race_name") \
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time","race_time")

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name","team")

# COMMAND ----------

# MAGIC %md
# MAGIC ### join circuits and races

# COMMAND ----------

races_circuits_df=races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id,"inner") \
    .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ### join races_circuits_df to all other

# COMMAND ----------

races_results_df=results_df.join(races_circuits_df,results_df.race_id==races_circuits_df.race_id) \
                           .join(drivers_df,results_df.driver_id==drivers_df.driver_id) \
                            .join(constructors_df,results_df.constructor_id==constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=races_results_df.select("race_name","race_year","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position") \
    .withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year==2020 and race_name=='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
