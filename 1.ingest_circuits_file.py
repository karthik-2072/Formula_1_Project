# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 1: Read the csv file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dbprojectstorage1/raw

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType,StructType,StructField,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",DoubleType(),True),
                                   StructField("url",StringType(),True),
                                   ])

# COMMAND ----------

circuits_df=spark.read \
.option("header",True) \
.schema(circuits_schema) \
.csv("/mnt/dbprojectstorage1/raw/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Rename the columns

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Adding new column(Ingestion Date)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df=circuits_renamed_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Write data to datalake as parquet file

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/dbprojectstorage1/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dbprojectstorage1/processed/circuits

# COMMAND ----------

df=spark.read.parquet("/mnt/dbprojectstorage1/processed/circuits")

# COMMAND ----------

display(df)
