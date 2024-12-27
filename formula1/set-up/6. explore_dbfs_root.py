# Databricks notebook source
# MAGIC %md 
# MAGIC #### Explore DBFS Root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 1. interact with DBFS file Browser
# MAGIC 1. Upload file to DBFS Root 

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

