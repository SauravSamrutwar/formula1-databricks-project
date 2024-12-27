# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
p_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_schema = "resultId INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, position INT, positionText STRING, positionOrder INT, points FLOAT,laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, fastestLapSpeed STRING, statusId INT"

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
        .json(f"{raw_folder_path}/{p_file_date}/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("constructorId", "constructor_id") \
                .withColumnRenamed("positionText", "position_text") \
                    .withColumnRenamed("positionOrder", "position_order") \
                        .withColumnRenamed("fastestLap", "fastest_lap") \
                            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("p_data_source", lit(p_data_source)) \
                                        .withColumn("p_file_date", lit(p_file_date))

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_renamed_df.drop(col("statusId")) \
    .withColumn("injestion_id", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #####De-duped Dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# for race_id_list in results_renamed_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.result")):
#         spark.sql(f"ALTER TABLE f1_processed.result DROP IF EXISTS PARTITION (race_id={race_id_list.race_id})")             

# COMMAND ----------

# results_renamed_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.result")

# COMMAND ----------

#overite_partition_column(results_renamed_df, 'f1_processed', 'result', 'race_id')

# COMMAND ----------

# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
# from delta.tables import DeltaTable

# if(spark._jsparkSession.catalog().tableExists(f"f1_processed.result")):
#     deltaTable = DeltaTable.forPath(spark, f"/mnt/formula1dl0903/processed/result")
#     deltaTable.alias("tgt").merge(
#         results_renamed_df.alias("src"),
#         "tgt.resultId = src.resultId AND tgt.race_id = src.race_id  ").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 
# else:
#     results_renamed_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable(f"f1_processed.result")

# COMMAND ----------

merge_condition = "tgt.resultId = src.resultId AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'result', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  COUNT(1)
# MAGIC FROM f1_processed.result
# MAGIC WHERE p_file_date = '2021-03-21';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.result
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1)>1
# MAGIC ORDER BY  race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.result
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")