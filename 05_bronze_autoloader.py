# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Bronze (incremental): Auto Loader
# MAGIC
# MAGIC Ingests **only new files** from the landing zone into `bronze_trips_incremental` using
# MAGIC Databricks Auto Loader (`cloudFiles`) with an `availableNow` trigger.
# MAGIC
# MAGIC - The **checkpoint** remembers which files were already loaded, so re-running the
# MAGIC   notebook with no new files loads 0 rows
# MAGIC - `_source_file` records which file every row came from (lineage)
# MAGIC - An explicit schema is used so bad files cannot silently change column types

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "nyc_taxi_lakehouse"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing/trips"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/landing/_checkpoints/bronze_trips_incremental"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_trips_incremental"

TRIP_SCHEMA = (
    "tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, "
    "trip_distance DOUBLE, fare_amount DOUBLE, pickup_zip INT, dropoff_zip INT"
)

# COMMAND ----------

rows_before = spark.table(BRONZE_TABLE).count() if spark.catalog.tableExists(BRONZE_TABLE) else 0

stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(TRIP_SCHEMA)
    .load(LANDING_PATH)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    stream.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)
query.awaitTermination()

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)
rows_after = bronze.count()
files_loaded = bronze.select("_source_file").distinct().count()

print(f"Rows before this run:   {rows_before:,}")
print(f"Rows loaded this run:   {rows_after - rows_before:,}")
print(f"Rows in Bronze now:     {rows_after:,}")
print(f"Files loaded in total:  {files_loaded:,}")
