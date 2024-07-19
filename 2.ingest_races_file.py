# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 1: Read the csv file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dbprojectstorage1/raw

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType,StructType,StructField,DoubleType,DateType

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                   StructField("year",IntegerType(),True),
                                   StructField("round",IntegerType(),True),
                                   StructField("circuitId",IntegerType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("date",DateType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("url",StringType(),True),
                                   ])

# COMMAND ----------

races_df=spark.read \
.option("header",True) \
.schema(races_schema) \
.csv("/mnt/dbprojectstorage1/raw/races.csv")

# COMMAND ----------

type(races_df)

# COMMAND ----------

races_df.show()

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Rename the columns

# COMMAND ----------

races_renamed_df=races_df.withColumnRenamed("raceId","race_id").withColumnRenamed("year","race_year").withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Adding new column(Ingestion Date)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,col,lit

# COMMAND ----------

races_final_df=races_renamed_df.withColumn("ingestion_date",current_timestamp()).withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df=races_final_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Write data to datalake as parquet file

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet("/mnt/dbprojectstorage1/processed/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dbprojectstorage1/processed/races

# COMMAND ----------

df=spark.read.parquet("/mnt/dbprojectstorage1/processed/races")

# COMMAND ----------

display(df)
