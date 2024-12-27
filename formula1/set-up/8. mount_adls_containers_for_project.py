# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake containers for the prject
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Get secrets from the key vault
    client_id = dbutils.secrets.get( scope = 'formula1-scope', key = 'formula1-app-client-id')
    tenent_id = dbutils.secrets.get( scope = 'formula1-scope', key = 'formula1-app-tenent-id')
    client_secret = dbutils.secrets.get( scope = 'formula1-scope', key = 'formula1-app-client-secret')
    #Set spark configuration 
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret":client_secret,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    #Mount the storafe account
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())


# COMMAND ----------

# MAGIC %md
# MAGIC ##Mount Raw Container

# COMMAND ----------

mount_adls("formula1dl0903", "raw")

# COMMAND ----------

mount_adls("formula1dl0903", "processed")

# COMMAND ----------

mount_adls("formula1dl0903", "presentation")

# COMMAND ----------

# MAGIC %md
# MAGIC