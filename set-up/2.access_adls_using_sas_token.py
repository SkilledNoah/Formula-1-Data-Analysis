# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. read data from circuits.csv file

# COMMAND ----------

noahformula1dl_demo_sas_token = dbutils.secrets.get(scope = "noahformula1-scope", key = "noahformula1-demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.noahformula1dl.dfs.core.windows.net","SAS")
spark.conf.set("fs.azure.sas.token.provider.type.noahformula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.noahformula1dl.dfs.core.windows.net", noahformula1dl_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@noahformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@noahformula1dl.dfs.core.windows.net/circuits.csv"))
