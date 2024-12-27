# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/formula1dl0903/demo"

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema", True) \
        .json("/mnt/formula1dl0903/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11-position
# MAGIC WHERE position <= 10

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark,"/mnt/formula1dl0903/demo/results_managed")
deltaTable.update("position <= 10", {"points": "21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark,"/mnt/formula1dl0903/demo/results_managed")
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

drivers_day1_df = spark.read \
    .option("inferSchema", True) \
        .json("/mnt/formula1dl0903/raw/2021-03-28/drivers.json") \
            .filter("driverId <= 10") \
                .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

drivers_day2_df = spark.read \
    .option("inferSchema", True) \
        .json("/mnt/formula1dl0903/raw/2021-03-28/drivers.json") \
            .filter("driverId BETWEEN 6 AND 15") \
                .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_df = spark.read \
    .option("inferSchema", True) \
        .json("/mnt/formula1dl0903/raw/2021-03-28/drivers.json") \
            .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
                .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET tgt.dob = upd.dob, tgt.forename = upd.forename, tgt.surname = upd.surname, tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp) 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET tgt.dob = upd.dob, tgt.forename = upd.forename, tgt.surname = upd.surname, tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl0903/demo/drivers_merge") 

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
        .whenMatchedUpdate(set = {"dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate" : "current_timestamp()"}) \
            .whenNotMatchedInsert(values={"driverId": "upd.driverId" ,"dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "createdDate" : "current_timestamp()"}) \
                .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;