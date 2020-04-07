# Databricks notebook source
from pyspark.sql.functions import col

spark.range(1000).withColumn("mod", col("id") % 10).groupBy("mod").count().show()

# COMMAND ----------

