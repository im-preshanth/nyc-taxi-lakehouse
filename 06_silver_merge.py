# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Silver (incremental): validate and MERGE
# MAGIC
# MAGIC Reads **only new Bronze rows** (a Delta stream with its own checkpoint), applies the
# MAGIC same data quality rules as the batch pipeline, and **upserts** valid trips into
# MAGIC `silver_trips_incremental` with `MERGE` on a deterministic `trip_id`.
# MAGIC
# MAGIC - Re-processing the same data never creates duplicates (idempotent)
# MAGIC - Failed rows are appended to `silver_trips_incremental_quarantine` with a reason

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "workspace"
SCHEMA = "nyc_taxi_lakehouse"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_trips_incremental"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_trips_incremental"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_trips_incremental_quarantine"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/landing/_checkpoints/silver_trips_incremental"

BUSINESS_COLS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip",
]

# COMMAND ----------

def apply_quality_rules(df):
    df = df.withColumn(
        "trip_duration_min",
        F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60, 2),
    )
    reason = (
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
    trip_id = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in BUSINESS_COLS]), 256)
    return df.withColumn("_dq_failure_reason", reason).withColumn("trip_id", trip_id)


def upsert_batch(batch_df, batch_id):
    session = batch_df.sparkSession
    checked = apply_quality_rules(batch_df)

    (checked.filter(F.col("_dq_failure_reason").isNotNull())
        .withColumn("_processed_at", F.current_timestamp())
        .write.mode("append").saveAsTable(QUARANTINE_TABLE))

    valid = (
        checked.filter(F.col("_dq_failure_reason").isNull())
        .drop("_dq_failure_reason")
        .dropDuplicates(["trip_id"])
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
        .withColumn("fare_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
        .withColumn("_processed_at", F.current_timestamp())
    )

    if not session.catalog.tableExists(SILVER_TABLE):
        valid.write.saveAsTable(SILVER_TABLE)
    else:
        (DeltaTable.forName(session, SILVER_TABLE).alias("t")
            .merge(valid.alias("s"), "t.trip_id = s.trip_id")
            .whenNotMatchedInsertAll()
            .execute())

# COMMAND ----------

silver_before = spark.table(SILVER_TABLE).count() if spark.catalog.tableExists(SILVER_TABLE) else 0

query = (
    spark.readStream.table(BRONZE_TABLE)
    .writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

silver_after = spark.table(SILVER_TABLE).count()
quarantine = spark.table(QUARANTINE_TABLE).count() if spark.catalog.tableExists(QUARANTINE_TABLE) else 0
duplicates = (spark.table(SILVER_TABLE).groupBy("trip_id").count().filter("count > 1").count())

print(f"Silver rows before this run: {silver_before:,}")
print(f"New trips merged this run:   {silver_after - silver_before:,}")
print(f"Silver rows now:             {silver_after:,}")
print(f"Quarantined rows (total):    {quarantine:,}")
print(f"Duplicate trip_ids in Silver: {duplicates}")
