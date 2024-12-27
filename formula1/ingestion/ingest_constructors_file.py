# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1 Read the constructor json file using dataframe schema

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
p_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING "

# COMMAND ----------

constuctor_df = spark.read \
    .schema(constructor_schema) \
        .json(f"{raw_folder_path}/{p_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, lit 

# COMMAND ----------

constuctor_dropped_df = constuctor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 3 Rename columns and add ingestion file

# COMMAND ----------

constructor_final_df = constuctor_dropped_df.withColumn('ingestion_date', current_timestamp()) \
    .withColumnRenamed("constructorId", "constructor_id") \
        .withColumnRenamed("constructorRef", "constructor_ref") \
            .withColumn("p_data_source", lit(p_data_source)) \
                .withColumn("p_file_date", lit(p_file_date))

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructor

# COMMAND ----------

dbutils.notebook.exit("Success")