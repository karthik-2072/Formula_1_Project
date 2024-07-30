# Databricks notebook source
# MAGIC %md
# MAGIC #### Access DataFrame using SQL
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Create global views on dataframes
# MAGIC 2. Access the views from sql cell
# MAGIC 3. Access the views from python cell
# MAGIC 4. Access the views in other notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Access the views from sql cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Access the views from python cell

# COMMAND ----------

p_race_year=2019

# COMMAND ----------

race_results_2019_df=spark.sql(f"SELECT * FROM global_temp.gv_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)
