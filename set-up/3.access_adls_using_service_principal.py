# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a secret / password for the Application
# MAGIC 1. Set Spark Config with App/Client Id, Directory / Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "noahformula1-scope", key = "noahformula1-app-client-id")
tenant_id = dbutils.secrets.get(scope = "noahformula1-scope", key = "noahformula1-app-tenant-id")
client_secret = dbutils.secrets.get(scope = "noahformula1-scope", key = "noahformula1-app-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.noahformula1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.noahformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.noahformula1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.noahformula1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.noahformula1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@noahformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@noahformula1dl.dfs.core.windows.net/circuits.csv"))
