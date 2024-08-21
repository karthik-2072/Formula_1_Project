-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### create Constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorID INT,
  constructorREF STRING,
  name STRING,
  nationality STRING,
  url STRING
)
using JSON
option (path "/mnt/dbprojectstorage1/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create drivers table
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverID INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename:STRING,surname: STRING>,
dob DATE,
nationality STRING,
url STRING,
)
using JSON
option (path "/mnt/dbprojectstorage1/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId String
)
using JSON
option (path "/mnt/dbprojectstorage1/raw/results.json")

-- COMMAND ----------



-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create pitstops table
-- MAGIC #### multi line json

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pitstops;
CREATE TABLE IF NOT EXISTS f1_raw.pitstops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING
)
using JSON
option (path "/mnt/dbprojectstorage1/raw/pitstops.json",multiline true)

-- COMMAND ----------

select * from f1_raw.pitstops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create lap times table(it is folder)
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
using csv
option (path "/mnt/dbprojectstorage1/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create qualifying table(it is multi line json,it is folder)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT
)
using json
option (path "/mnt/dbprojectstorage1/raw/qualifying",multiline true)

-- COMMAND ----------

select * from f1_raw.qualifying;
