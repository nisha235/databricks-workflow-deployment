# Databricks notebook source
df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/Most_Streamed_Spotify_Songs_2024.csv")
display(df)


# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

views_list = ['Spotify Streams', 'YouTube Views', 'TikTok Views']
for x in views_list:
    df = df.withColumn(x, regexp_replace(col(x), ',', ''))

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("csv").option("header", "true").mode('overwrite').save("dbfs:/FileStore/Processed_Most_Streamed_Spotify_Songs_2024/")

# COMMAND ----------


