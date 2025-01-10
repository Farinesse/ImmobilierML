# Databricks notebook source
# MAGIC %pip install ydata-profiling

# COMMAND ----------

from ydata_profiling import ProfileReport

# COMMAND ----------

df = spark.read.format("delta").table("default.immobilier_analysis")

# COMMAND ----------

profile = ProfileReport(df)

# COMMAND ----------

profile.to_notebook_iframe()

# COMMAND ----------


