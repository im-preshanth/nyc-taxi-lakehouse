# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: clean and validate taxi trips
# MAGIC
# MAGIC **Layer:** Silver (cleaned, validated)
# MAGIC **Source:** `workspace.nyc_taxi_lakehouse.bronze_trips`
# MAGIC **Targets:**
# MAGIC - `silver_trips`: rows that pass every data quality rule, with derived columns
# MAGIC - `silver_trips_quarantine`: rows that fail, with the reason, so nothing is silently dropped
# MAGIC
# MAGIC Data quality rules:
# MAGIC 1. Pickup time, dropoff time, distance and fare are not null
# MAGIC 2. Dropoff is after pickup
# MAGIC 3. Trip duration is at most 6 hours
# MAGIC 4. Distance is greater than 0 and at most 100 miles
# MAGIC 5. Fare is greater than 0 and at most 500
# MAGIC 6. Exact duplicate trips are removed

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "nyc_taxi_lakehouse"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_trips"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_trips"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_trips_quarantine"

BUSINESS_COLS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip",
]

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)

# Trip duration in minutes, used by the rules and kept as a column
with_duration = bronze.withColumn(
    "trip_duration_min",
    F.round(
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
        2,
    ),
)

# First failing rule for each row (null when the row passes every rule)
dq_reason = (
    F.when(
        F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull()
        | F.col("trip_distance").isNull() | F.col("fare_amount").isNull(),
        "missing_required_field",
    )
    .when(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"), "dropoff_not_after_pickup")
    .when(F.col("trip_duration_min") > 360, "duration_over_6_hours")
    .when((F.col("trip_distance") <= 0) | (F.col("trip_distance") > 100), "distance_out_of_range")
    .when((F.col("fare_amount") <= 0) | (F.col("fare_amount") > 500), "fare_out_of_range")
)

checked = with_duration.withColumn("_dq_failure_reason", dq_reason)

# COMMAND ----------

# Valid rows: dedupe, then add derived columns for analysis
silver = (
    checked
    .filter(F.col("_dq_failure_reason").isNull())
    .dropDuplicates(BUSINESS_COLS)
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
    .withColumn("fare_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
    .withColumn("_processed_at", F.current_timestamp())
    .drop("_dq_failure_reason")
)

quarantine = (
    checked
    .filter(F.col("_dq_failure_reason").isNotNull())
    .withColumn("_processed_at", F.current_timestamp())
)

(silver.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(SILVER_TABLE))
(quarantine.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(QUARANTINE_TABLE))

# COMMAND ----------

# Data quality summary
bronze_count = bronze.count()
silver_count = spark.table(SILVER_TABLE).count()
quarantine_count = spark.table(QUARANTINE_TABLE).count()
duplicates_removed = bronze_count - silver_count - quarantine_count

print(f"Bronze rows:          {bronze_count:,}")
print(f"Silver rows (valid):  {silver_count:,}")
print(f"Quarantined rows:     {quarantine_count:,}")
print(f"Duplicates removed:   {duplicates_removed:,}")
print(f"Pass rate:            {silver_count / bronze_count:.1%}")

display(
    spark.table(QUARANTINE_TABLE)
    .groupBy("_dq_failure_reason").count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(spark.table(SILVER_TABLE).limit(10))
