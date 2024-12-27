# Databricks notebook source
# MAGIC %md 
# MAGIC ####Read the JSON file using spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
p_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                 ])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("dob", DateType(), True),
                                    StructField("name",name_schema),
                                    StructField("nationality", StringType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("number",IntegerType(), True),
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(driver_schema) \
        .json(f"{raw_folder_path}/{p_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Rename the column and add new columns 

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, concat, lit

# COMMAND ----------

drivers_df_with_columns = drivers_df.withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("driverRef","driver_ref") \
        .withColumn("ingestionTime", current_timestamp()) \
            .withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname"))) \
                .withColumn("p_data_source", lit(p_data_source)) \
                    .withColumn("p_file_date", lit(p_file_date))


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 Droping Columns

# COMMAND ----------

drivers_final_df = drivers_df_with_columns.drop(col("url"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 4 Write to output to processed container in parquet format 

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.driver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.driver

# COMMAND ----------

dbutils.notebook.exit("Success")