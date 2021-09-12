# Databricks notebook source
# /FileStore/tables/ratings.csv

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/ratings.csv", header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.columns

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

#Q1. showing columns userid, movieId and timestamp column present in form of date for those movies whose rate is in between 3.0 and 5.0
df.select("userid", "movieId", "timestamp").where((df["rating"] >= 3) & (df["rating"] <= 5)).collect()

# COMMAND ----------

#Q2. Printing movies which were made after the year 2000
from pyspark.sql import functions as func

# COMMAND ----------

df1 = df.withColumnRenamed("timestamp", "time").withColumn("timestamp", func.from_unixtime("time"))
df1.show()

# COMMAND ----------

df1.select("userid", "movieId", "timestamp").filter(df1["timestamp"] > "2000-07-30 18:45:03").collect()

# COMMAND ----------

#Q3. Showing the dataframe of the total count of movies for each movie id that's created between the year 2000 and 2001
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

df1.select(count("movieId")).groupBy("movieId")
df1.show()
