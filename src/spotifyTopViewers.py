# Databricks notebook source
df = spark.read.option('header', 'true').csv('dbfs:/FileStore/Processed_Most_Streamed_Spotify_Songs_2024/')
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum

# Fill NaN values with 0
views_list = ['Spotify Streams', 'YouTube Views', 'TikTok Views']

df = df.fillna(0, subset=views_list)
# Convert columns to integer type
for x in views_list:
    df = df.withColumn(x, col(x).cast("long"))

# Create a new column 'Total Views' by summing the specified columns
df = df.withColumn("Total Views", col("Spotify Streams") + col("YouTube Views") + col("TikTok Views"))
df = df.withColumn("Total Views", col("Total Views").cast("long"))

display(df)

# COMMAND ----------


