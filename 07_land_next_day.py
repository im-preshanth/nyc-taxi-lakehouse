# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Simulate a new day of data arriving
# MAGIC
# MAGIC Lands the **next day** that is not yet in the landing zone, as a new JSON folder.
# MAGIC Run this, then run the job: Bronze should load only that day's file and Silver should
# MAGIC merge only that day's trips.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "nyc_taxi_lakehouse"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing/trips"

landed = {
    f.name.rstrip("/").split("=", 1)[1]
    for f in dbutils.fs.ls(LANDING_PATH)
    if f.name.startswith("pickup_date=")
}

source = (
    spark.read.table("samples.nyctaxi.trips")
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
)
all_dates = sorted(str(r["pickup_date"]) for r in source.select("pickup_date").distinct().collect())
pending = [d for d in all_dates if d not in landed]

if not pending:
    print("All days are already in the landing zone. Re-run 04_setup_landing_zone to reset.")
else:
    next_day = pending[0]
    day_df = source.filter(F.col("pickup_date") == F.lit(next_day)).drop("pickup_date")
    day_df.write.mode("overwrite").json(f"{LANDING_PATH}/pickup_date={next_day}")
    print(f"Landed {day_df.count():,} trips for {next_day}")
    print(f"Days still to arrive: {len(pending) - 1}")
