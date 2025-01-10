# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

(2_000_000 / 163_000_000)

# COMMAND ----------

df = spark.read.format("delta").table("default.immobilier_analysis").sample(0.01226,3)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("default.immobilier_analysis_2M")

# COMMAND ----------


nan_count = df.select(*[count(when((isnull(col_name) | isnan(col_name)) , col_name)).alias(col_name) for col_name in df.columns])

nan_count.display()

# COMMAND ----------

(34879 / 2000000)*100

# COMMAND ----------

df.filter(col("n_logements").isNull()).display()

# COMMAND ----------


