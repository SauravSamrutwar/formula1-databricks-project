# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
p_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuit") \
    .withColumnRenamed("name", "circuit_name") \
        .withColumnRenamed("location", "circuit_location") \
            .withColumnRenamed("circuitId", "circuit_id") 

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/race") \
    .withColumnRenamed("name", "race_name") \
        .withColumnRenamed("date", "race_date")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/driver") \
    .withColumnRenamed("name", "driver_name") \
        .withColumnRenamed("number", "driver_number") \
        .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/result") \
    .filter(f"p_file_date = '{p_file_date}'") \
        .withColumnRenamed("race_id", "results_race_id") \
            .withColumnRenamed("p_file_date", "results_file_date")

# COMMAND ----------

constructed_df = spark.read.format("delta").load(f"{processed_folder_path}/constructor") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(races_df.race_year, races_df.race_id, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df, results_df.results_race_id == races_circuits_df.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
        .join(constructed_df, results_df.constructor_id == constructed_df.constructor_id)

# COMMAND ----------

#display(race_results_df.filter("race_year == 2019 and race_name == 'Abu Dhabi Grand Prix'").orderBy(results_df.points.desc()))

# COMMAND ----------

#race_results_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

#display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team","grid", "fastest_lap","time", "points", "position", "results_file_date") \
    .withColumn("created_date", current_timestamp())

# COMMAND ----------

#overite_partition_column(final_df, 'f1_presentation', 'race_result', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_result', presentation_folder_path, merge_condition, 'race_id') 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_result ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_result
# MAGIC GROUP BY race_id
# MAGIC ORDER BY  race_id DESC;