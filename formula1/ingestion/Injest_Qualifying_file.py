# Databricks notebook source
# MAGIC %md 
# MAGIC ####Step 1 Read the Multiline JSON file

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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)
                                       ])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
        .option("multiline", True) \
            .json(f"{raw_folder_path}/{p_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 Rename the columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualifying_id") \
    .withColumnRenamed("raceId", "race_id") \
        .withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("constructorId", "constructor_id") \
                .withColumn("injestion_date", current_timestamp()) \
                    .withColumn("p_data_source", lit(p_data_source)) \
                        .withColumn("p_file_date", lit(p_file_date))

# COMMAND ----------

#overite_partition_column(final_df, 'f1_processed', 'qualifying_file', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualifying_id = src.qualifying_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying_file', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying_file
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 write to output to processed container in parquet format

# COMMAND ----------

#final_df.write.mode("overwrite").parquet("/mnt/formula1dl0903/raw/qualifying")

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1dl0903/raw/qualifying"))