# Databricks notebook source
# MAGIC %md
# MAGIC #Explore the Capabilities of the dbutils.secrets.utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope', key='formula1-dl-account-key')

# COMMAND ----------

