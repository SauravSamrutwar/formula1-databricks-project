# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl0903.dfs.core.windows.net",
    "T9eEWcbnR8cg8me8VeA1Wy0EeBPkmIiAssbqckCKOOFr9+NJBoEHBSq+uwgPv7bw57ZTABKAWg74+AStZWuNPg=="
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0903.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl0903.dfs.core.windows.net/circuits.csv"))