# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Dataframes using SQL
# MAGIC 1. Create Temporary views on dataframes
# MAGIC 1. Access the view from sql cell
# MAGIC 1. Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results.createTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)