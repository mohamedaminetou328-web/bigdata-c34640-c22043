# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, count, broadcast, to_date
import time

spark = SparkSession.builder \
    .appName("NYC Taxi Weather Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=" * 60)
print("NYC Taxi & Weather Data Analysis")
print("=" * 60)

# Read data from HDFS
print("\n[1] Reading data from HDFS...")
taxi_df = spark.read.parquet("hdfs://my-namenode:8020/user/data/taxi/taxi.parquet")
weather_df = spark.read.parquet("hdfs://my-namenode:8020/user/data/weather/weather.parquet")

taxi_count = taxi_df.count()
weather_count = weather_df.count()
print("    -> Number of taxi trips: " + str(taxi_count))
print("    -> Number of weather records: " + str(weather_count))

# ============================================================
# TEST 1: BROADCASTHASHJOIN
# ============================================================
print("\n" + "=" * 60)
print("[2] TEST 1: BROADCASTHASHJOIN (Optimized)")
print("=" * 60)

start_broadcast = time.time()
joined_broadcast = taxi_df.join(
    broadcast(weather_df),
    to_date(col("pickup_datetime")) == col("date")
)
joined_broadcast.count()
time_broadcast = time.time() - start_broadcast

print("\n[2.1] Execution plan (BroadcastHashJoin):")
joined_broadcast.explain(mode="simple")
print("\n[2.2] Execution time: " + str(round(time_broadcast, 2)) + " seconds")

# ============================================================
# TEST 2: SORTMERGEJOIN (without broadcast)
# ============================================================
print("\n" + "=" * 60)
print("[3] TEST 2: SORTMERGEJOIN (Without optimization)")
print("=" * 60)

# Disable automatic broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

start_sortmerge = time.time()
joined_sortmerge = taxi_df.join(
    weather_df,
    to_date(col("pickup_datetime")) == col("date")
)
joined_sortmerge.count()
time_sortmerge = time.time() - start_sortmerge

print("\n[3.1] Execution plan (SortMergeJoin with Shuffle):")
joined_sortmerge.explain(mode="simple")
print("\n[3.2] Execution time: " + str(round(time_sortmerge, 2)) + " seconds")