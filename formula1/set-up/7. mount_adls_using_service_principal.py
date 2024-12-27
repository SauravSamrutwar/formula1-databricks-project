# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using service principal
# MAGIC steps to follow
# MAGIC
# MAGIC 1. Get client id , tenent id, and client secret from key vault 
# MAGIC 1. Set Spark Config With App/Client id, Directory/ tenent id and secret
# MAGIC 1. Call file system utility mount to mount storage
# MAGIC 1. Explore other file system utilities related to mount(list all mount, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get( scope = 'formula1-scope', key = 'formula1-app-client-id')
tenent_id = dbutils.secrets.get( scope = 'formula1-scope', key = 'formula1-app-tenent-id')
client_secret = dbutils.secrets.get( scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl0903.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl0903/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("mnt/formula1dl0903/demo"))

# COMMAND ----------

display(dbutils.fs.ls("mnt/formula1dl0903/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.unmount("/mnt/formula1dl0903/demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC