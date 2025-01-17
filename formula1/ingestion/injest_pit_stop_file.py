# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1 Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
p_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stop_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("stop", IntegerType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("duration", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True)
                                       ])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Handling multiline JSON

# COMMAND ----------

pit_stop_df = spark.read \
    .schema(pit_stop_schema) \
        .option("multiline", "true") \
        .json(f"{raw_folder_path}/{p_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 Rename the columns and new columns 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = pit_stop_df.withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("raceId", "race_id") \
    .withColumn("injestion_date", current_timestamp()) \
      .withColumn("p_data_source", lit(p_data_source)) \
        .withColumn("p_file_date", lit(p_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Step 3 Write to output to processed container in parquet format

# COMMAND ----------

#overite_partition_column(final_df, 'f1_processed', 'pit_stop', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stop', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.pit_stop
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

#final_df.write.mode("overwrite").parquet("/mnt/formula1dl0903/processed/pit_stops")

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1dl0903/processed/pit_stops"))