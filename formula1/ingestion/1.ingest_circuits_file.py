# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest Circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
        .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitsId", IntegerType(), False),
                             StructField("circuitRef", StringType(), True),
                             StructField("name", StringType(), True),
                             StructField("location", StringType(), True),
                             StructField("country", StringType(), True),
                             StructField("lat", DoubleType(), True),
                             StructField("lng", DoubleType(), True),
                             StructField("alt", IntegerType(), True),
                             StructField("url", StringType(), True),
                             
                             ])

# COMMAND ----------

# MAGIC %md 
# MAGIC #Select only the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitsId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
        .withColumnRenamed("lat", "latitute") \
        .withColumnRenamed("lng", "longitute") \
            .withColumnRenamed("alt", "altitude") \
                .withColumn("p_data_source", lit(v_data_source)) \
                    .withColumn("p_file_date", lit(v_file_date))
                

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
    

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuit

# COMMAND ----------

dbutils.notebook.exit("Success")