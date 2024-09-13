# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = "noahformula1-scope")

# COMMAND ----------

dbutils.secrets.get(scope = "noahformula1-scope", key = "noahformula1dl-account-key")
