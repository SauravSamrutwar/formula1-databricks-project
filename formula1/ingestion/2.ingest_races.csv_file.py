# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC #Reading the files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
p_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False), 
                                  StructField("year", IntegerType(), True), 
                                  StructField("round", IntegerType(), True),
                                   StructField("circuitId", IntegerType(), True), 
                                   StructField("name", StringType(), True), 
                                   StructField("date", StringType(), True ), 
                                   StructField("time", StringType(), True),
                                   StructField("url", StringType(), True)
                                   ])

# COMMAND ----------

races_df = spark.read \
    .option("header", "true") \
           .schema(races_schema) \
            .csv(f"{raw_folder_path}/{p_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # select the required columns

# COMMAND ----------

races_selected_df = races_df.select("raceId", "year", "round", "circuitId", "name", "date", "time")

# COMMAND ----------

# MAGIC %md
# MAGIC # renaming the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_remaned_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
        .withColumnRenamed("circuitId", "circuit_id") \
            .withColumn("p_data_source", lit(p_data_source)) \
                .withColumn("p_file_date", lit(p_file_date)) 

# COMMAND ----------

from pyspark.sql.functions import col, concat, to_timestamp, current_timestamp

# COMMAND ----------

races_concat_df = races_remaned_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '), col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_remaned_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.race")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.race

# COMMAND ----------

dbutils.notebook.exit("Success")