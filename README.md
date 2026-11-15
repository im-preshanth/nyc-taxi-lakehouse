# NYC Taxi Lakehouse Pipeline (Databricks · PySpark · Delta Lake)

An end-to-end **Medallion architecture** (Bronze → Silver → Gold) data pipeline built on Databricks with PySpark and Delta Lake. It ingests raw NYC taxi trip data, validates and cleans it with explicit data quality rules, quarantines bad records instead of dropping them, and publishes business-ready aggregate tables. The three stages run as a single orchestrated Databricks Job.

## Architecture

```mermaid
flowchart LR
    A[samples.nyctaxi.trips<br/>raw source] --> B[Bronze<br/>bronze_trips]
    B --> C{Data quality<br/>rules}
    C -->|pass| D[Silver<br/>silver_trips]
    C -->|fail| E[Quarantine<br/>silver_trips_quarantine]
    D --> F[Gold<br/>gold_daily_trip_summary]
    D --> G[Gold<br/>gold_hourly_demand]
    D --> H[Gold<br/>gold_pickup_zip_performance]
```

All tables are Delta tables in Unity Catalog under `workspace.nyc_taxi_lakehouse`.

| Layer | Notebook | Output | Purpose |
|---|---|---|---|
| Bronze | `01_bronze_ingest` | `bronze_trips` | Raw data as received, plus `_ingested_at` and `_source` audit columns |
| Silver | `02_silver_clean` | `silver_trips`, `silver_trips_quarantine` | Validated, deduplicated trips with derived columns; failed rows kept with a reason |
| Gold | `03_gold_aggregates` | 3 aggregate tables | Business-ready metrics, with a reconciliation check against Silver |

## Data quality rules (Silver)

Each row is checked against the rules below. Rows that fail are written to `silver_trips_quarantine` with a `_dq_failure_reason` column, so nothing is silently discarded and every rejection can be audited.

1. Pickup time, dropoff time, distance and fare must not be null
2. Dropoff must be after pickup
3. Trip duration must be 6 hours or less
4. Distance must be greater than 0 and at most 100 miles
5. Fare must be greater than 0 and at most 500
6. Exact duplicate trips are removed

Derived columns: `trip_duration_min`, `pickup_date`, `pickup_hour`, `pickup_day_of_week`, `fare_per_mile`.

## Results from the latest run

| Metric | Value |
|---|---|
| Bronze rows ingested | 21,932 |
| Silver rows (passed all rules) | 21,815 |
| Quarantined rows | 117 |
| Duplicates removed | 0 |
| Data quality pass rate | 99.5% |
| `gold_daily_trip_summary` | 60 rows |
| `gold_hourly_demand` | 168 rows (7 days × 24 hours) |
| `gold_pickup_zip_performance` | 120 rows |
| Reconciliation (Gold vs Silver trip count) | Passed: 21,815 = 21,815 |
| End-to-end job runtime (serverless) | ~1 min 17 s |

## Gold tables

- **`gold_daily_trip_summary`**: trip count, total fare, average fare, average distance and average duration per day. Answers: how is volume and revenue trending?
- **`gold_hourly_demand`**: trip count and average fare by day of week and hour. Answers: when is demand highest?
- **`gold_pickup_zip_performance`**: trips, total fare and average fare per mile by pickup ZIP, ranked by revenue. Answers: which areas generate the most revenue?

The Gold notebook ends with a **reconciliation check** that asserts the total trips in Gold equal the trips in Silver, so the job fails loudly if data is lost between layers.

## Orchestration

The notebooks run as one Databricks Job, `nyc_taxi_lakehouse_pipeline`, on serverless compute:

```
bronze_ingest  →  silver_clean  →  gold_aggregates
```

Each task depends on the previous one, so Silver only runs after a successful Bronze load and Gold only after a successful Silver load.

## Incremental pipeline (v2): Auto Loader + MERGE

The batch pipeline rebuilds every table on every run. Version 2 processes **only new data**, the way production pipelines do.

```mermaid
flowchart LR
    L[Landing volume<br/>one JSON folder per day] -->|Auto Loader<br/>new files only| B[bronze_trips_incremental]
    B -->|Delta stream<br/>new rows only| Q{Data quality<br/>rules}
    Q -->|pass: MERGE on trip_id| S[silver_trips_incremental]
    Q -->|fail: append| X[silver_trips_incremental_quarantine]
```

| Notebook | Purpose |
|---|---|
| `04_setup_landing_zone` | Creates a Unity Catalog volume and lands the first 50 days of trips as one JSON folder per day, simulating a source system |
| `05_bronze_autoloader` | Auto Loader (`cloudFiles`, `availableNow` trigger) ingests only files it has not seen before; a checkpoint tracks processed files and `_source_file` records lineage |
| `06_silver_merge` | Streams only new Bronze rows, applies the same quality rules, and upserts with `MERGE` on a SHA-256 `trip_id`, so re-processing never creates duplicates |
| `07_land_next_day` | Drops the next day's file into the landing zone to test an incremental run |

The two incremental notebooks run as a second Databricks Job, `nyc_taxi_incremental_pipeline`: `bronze_autoloader → silver_merge`.

### Proving it only processes new data

| Run | What landed | Bronze rows written |
|---|---|---|
| Initial load | 50 daily files | 18,063 (Silver: 17,964 valid, 99 quarantined, 0 duplicate `trip_id`s) |
| New day arrives (2016-02-20) | 1 new file | 414 (only the new file) |
| Re-run with no new data | nothing | 0 |

Key techniques: Auto Loader with checkpoints, Structured Streaming `availableNow` triggers, `foreachBatch` with Delta `MERGE`, deterministic hash keys for idempotency, explicit schemas, and file-level lineage.

## How to run it

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. In your workspace, go to **Workspace → Home → ⋮ → Import** and import the `.py` notebooks from this repo.
3. **Batch pipeline:** run `01_bronze_ingest`, `02_silver_clean` and `03_gold_aggregates` in order on **Serverless** compute, or create a Job with the three notebooks as dependent tasks.
4. **Incremental pipeline:** run `04_setup_landing_zone` once, then `05_bronze_autoloader` and `06_silver_merge`. Run `07_land_next_day` to simulate a new day, then run 05 and 06 again and compare the row counts.
5. Query the results, for example:
   ```sql
   SELECT * FROM workspace.nyc_taxi_lakehouse.gold_pickup_zip_performance ORDER BY revenue_rank LIMIT 10;
   ```

The source data is `samples.nyctaxi.trips`, a sample dataset built into every Databricks workspace, so no downloads or credentials are needed.

## Tech stack

Databricks (Free Edition, serverless) · PySpark · Delta Lake · Auto Loader · Structured Streaming · Delta MERGE · Unity Catalog (tables and volumes) · Databricks Jobs · SQL

## What I would change for production

- **Incremental loads**: done in v2 for Bronze and Silver (see above); the next step is building Gold incrementally from Silver changes.
- **Scalable ranking**: the ZIP revenue ranking uses an unpartitioned window, which is fine for 120 aggregated rows but would be partitioned (for example by region or month) at larger scale.
- **Declarative quality checks**: move the rules into Lakeflow Declarative Pipelines expectations to get built-in data quality metrics.
- **Alerting**: add job failure notifications and a threshold alert if the quarantine rate rises above an agreed level.
- **CI/CD**: deploy the notebooks and job definition with Databricks Asset Bundles from Git.

## Author

**Preshanth Dhanapal**, Data Engineer
[LinkedIn](https://www.linkedin.com/in/preshanth-d-ab0302421)
