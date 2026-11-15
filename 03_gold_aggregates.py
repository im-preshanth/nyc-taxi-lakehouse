# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: business-ready aggregates
# MAGIC
# MAGIC **Layer:** Gold (aggregated, ready for BI)
# MAGIC **Source:** `workspace.nyc_taxi_lakehouse.silver_trips`
# MAGIC **Targets:**
# MAGIC - `gold_daily_trip_summary`: trips, revenue and averages per day
# MAGIC - `gold_hourly_demand`: demand by day of week and hour
# MAGIC - `gold_pickup_zip_performance`: revenue and efficiency by pickup ZIP code

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "nyc_taxi_lakehouse"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_trips"

silver = spark.table(SILVER_TABLE)

def save(df, name):
    (df.withColumn("_built_at", F.current_timestamp())
       .write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    print(f"Saved {CATALOG}.{SCHEMA}.{name}: {spark.table(f'{CATALOG}.{SCHEMA}.{name}').count():,} rows")

# COMMAND ----------

# 1. Daily trip summary
daily = (
    silver.groupBy("pickup_date")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
    )
    .orderBy("pickup_date")
)
save(daily, "gold_daily_trip_summary")

# COMMAND ----------

# 2. Hourly demand by day of week
hourly = (
    silver.groupBy("pickup_day_of_week", "pickup_hour")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    )
    .orderBy("pickup_day_of_week", "pickup_hour")
)
save(hourly, "gold_hourly_demand")

# COMMAND ----------

# 3. Pickup ZIP performance, ranked by revenue
zip_perf = (
    silver.filter(F.col("pickup_zip").isNotNull())
    .groupBy("pickup_zip")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
    )
    .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.desc("total_fare"))))
    .orderBy("revenue_rank")
)
save(zip_perf, "gold_pickup_zip_performance")

# COMMAND ----------

# Reconciliation check: Gold totals must match Silver exactly
silver_trips = silver.count()
gold_trips = spark.table(f"{CATALOG}.{SCHEMA}.gold_daily_trip_summary").agg(F.sum("trip_count")).first()[0]
assert silver_trips == gold_trips, f"Mismatch: silver={silver_trips}, gold={gold_trips}"
print(f"Reconciliation passed: {gold_trips:,} trips in Gold match Silver")

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.gold_pickup_zip_performance").limit(10))
