# Databricks notebook source
dbutils.fs.mount(
source = "wasbs://containergroup8@stockagegroup8.blob.core.windows.net",
mount_point = "/mnt/monblob",
extra_configs = {"fs.azure.account.key.stockagegroup8.blob.core.windows.net":dbutils.secrets.get(scope ="group8secretscope", key = "secretkeygroup8")})

# COMMAND ----------

display(dbutils.fs.ls("/mnt/monblob"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/monblob/flux_nouveaux_emprunts.csv"))

# COMMAND ----------

spark.read.format("csv").option("header", "true").load("dbfs:/mnt/monblob/flux_nouveaux_emprunts.csv").

# COMMAND ----------


