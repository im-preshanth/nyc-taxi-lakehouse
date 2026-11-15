# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: ingest raw NYC taxi trips
# MAGIC
# MAGIC **Layer:** Bronze (raw)
# MAGIC **Source:** `samples.nyctaxi.trips` (built-in Databricks sample data)
# MAGIC **Target:** `workspace.nyc_taxi_lakehouse.bronze_trips` (Delta table)
# MAGIC
# MAGIC Bronze keeps the data exactly as received, plus two audit columns:
# MAGIC when it was ingested and where it came from. No cleaning happens here.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "nyc_taxi_lakehouse"
SOURCE_TABLE = "samples.nyctaxi.trips"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_trips"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

raw = spark.read.table(SOURCE_TABLE)

bronze = (
    raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit(SOURCE_TABLE))
)

(
    bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

# COMMAND ----------

# Quick checks: row count, schema, and a sample of rows
bronze_df = spark.table(BRONZE_TABLE)
print(f"Rows loaded into {BRONZE_TABLE}: {bronze_df.count():,}")
bronze_df.printSchema()
display(bronze_df.limit(10))
