-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("delta").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

desc extended demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_2020;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_location = f"abfss://presentation@formula1dl0903.dfs.core.windows.net/race_results_ext"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("parquet").option("path",f"{file_location}").saveAsTable("demo.race_results_ext")

-- COMMAND ----------

DESC EXTENDED  demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date STRING,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING PARQUET
LOCATION "gs:/mnt/formula1dl0903/presentation/race_results_ext_sql"

-- COMMAND ----------

CREATE TEMP VIEW v_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

CREATE VIEW v_race_results
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES;