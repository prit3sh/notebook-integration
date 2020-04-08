# Databricks notebook source
dbutils.widgets.text("input_path", "some/default/input/path")

# COMMAND ----------

# MAGIC %run "./utils/udfs/CustomUDFs"

# COMMAND ----------

from pyspark.sql.functions import col

input_path = dbutils.widgets.get("input_path")

spark.range(1000).withColumn("mod", col("id") % 10).groupBy("mod").count().show()

print(input_path)

# COMMAND ----------

rdd = sc.parallelize([[1, "hello everybody"], [2, "badger badger badger mushroom mushroom"]])
df = spark.createDataFrame(rdd, "id int, words string")
display(df.select("id", tokenizeUDF(col("words"))))

# COMMAND ----------

