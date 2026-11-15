# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Setup: landing zone for incremental loads
# MAGIC
# MAGIC Simulates a real source system that drops a new file of trips every day.
# MAGIC
# MAGIC - Creates a Unity Catalog **volume** called `landing` (cloud file storage)
# MAGIC - Splits `samples.nyctaxi.trips` into **one JSON folder per pickup date**
# MAGIC - Lands only the **first 50 days** now; the remaining days are landed one at a time later
# MAGIC   (notebook `07_land_next_day`) to prove the pipeline processes only new data
# MAGIC
# MAGIC Run this notebook **once**. Re-running it resets the landing zone to the first 50 days.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "nyc_taxi_lakehouse"
VOLUME = "landing"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/trips"
INITIAL_DAYS = 50

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

source = (
    spark.read.table("samples.nyctaxi.trips")
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
)

all_dates = [r["pickup_date"] for r in source.select("pickup_date").distinct().orderBy("pickup_date").collect()]
initial_dates = all_dates[:INITIAL_DAYS]
cutoff = initial_dates[-1]

print(f"Source covers {len(all_dates)} days: {all_dates[0]} to {all_dates[-1]}")
print(f"Landing the first {len(initial_dates)} days now (up to {cutoff})")
print(f"{len(all_dates) - len(initial_dates)} days are held back for incremental runs")

# COMMAND ----------

# One folder per day, e.g. .../trips/pickup_date=2016-01-01/part-....json
(
    source
    .filter(F.col("pickup_date") <= F.lit(cutoff))
    .write
    .mode("overwrite")
    .partitionBy("pickup_date")
    .json(LANDING_PATH)
)

# COMMAND ----------

landed = [f.name for f in dbutils.fs.ls(LANDING_PATH) if f.name.startswith("pickup_date=")]
print(f"Day folders in landing zone: {len(landed)}")
print("First:", sorted(landed)[0], " Last:", sorted(landed)[-1])
