# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. read data from circuits.csv file

# COMMAND ----------

noahformula1dl_account_key = dbutils.secrets.get(scope = "noahformula1-scope", key = "noahformula1dl-account-key")

# COMMAND ----------

spark.conf.set(

    "fs.azure.account.key.noahformula1dl.dfs.core.windows.net",
    noahformula1dl_account_key)



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@noahformula1dl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@noahformula1dl.dfs.core.windows.net/circuits.csv"))
