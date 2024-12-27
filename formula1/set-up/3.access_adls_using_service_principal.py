# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using service principal
# MAGIC steps to follow
# MAGIC 1. Register Azure AD Application
# MAGIC 1. Generate a secret/password for the Application
# MAGIC 1. Set Spark Config with App/Client id Directory/Tenent Id and Secret
# MAGIC 1. Assign role 'storage bob data contributor' to the data lake. 

# COMMAND ----------

client_id = dbutils.secrets.get( scope = 'formula1-scope', key = 'formula1-app-client-id')
tenent_id = dbutils.secrets.get( scope = 'formula1-scope', key = 'formula1-app-tenent-id')
client_secret = dbutils.secrets.get( scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formula1dl0903.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl0903.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl0903.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl0903.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl0903.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0903.dfs.core.windows.net"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl0903.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC