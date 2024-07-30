# Databricks notebook source
# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits") \
    .filter("circuit_id<70") \
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019") \
    .withColumnRenamed("name","race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"inner")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select only required rows

# COMMAND ----------

races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"inner") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

display(races_circuits_df.select("race_name","country"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outer Join

# COMMAND ----------

#left outer join
races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"left") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

#right outer join
races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"right") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

#full outer join
races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"full") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi Join

# COMMAND ----------

races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"semi") \
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"semi")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti join

# COMMAND ----------

races_circuits_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id,"anti")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross join

# COMMAND ----------

races_circuits_df=races_df.crossJoin(circuits_df)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df.count()
