# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using sas token
# MAGIC 1. Set the spark config for sas token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

formula1dl_account_key= dbutils.secrets.get( scope = 'formula1-scope', key = 'formula-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl0903.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl0903.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl0903.dfs.core.windows.net", formula1dl_account_key)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0903.dfs.core.windows.net"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0903.dfs.core.windows.net"))