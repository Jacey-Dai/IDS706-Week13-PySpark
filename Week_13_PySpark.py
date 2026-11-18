# Databricks notebook source
# MAGIC %md
# MAGIC # Flight delays at major US airports, 2005–2008
# MAGIC The source is the ASA airline on-time dataset distributed with Databricks. This pipeline prepares a Parquet snapshot of 1999–2008 and analyzes scheduled departures from ATL, ORD, and DFW.

# COMMAND ----------

from time import perf_counter
from pyspark.sql import functions as F

source = "/databricks-datasets/asa/airlines/{1999,200*}.csv"
volume = "workspace.default.week13_flight_analysis"
root = "/Volumes/workspace/default/week13_flight_analysis"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume}")

raw = spark.read.option("header", True).csv(source)
typed = raw.selectExpr(
    "CAST(Year AS INT) AS year",
    "CAST(Month AS INT) AS month",
    "Origin AS origin",
    "Dest AS destination",
    "CAST(Cancelled AS INT) AS cancelled",
    "TRY_CAST(DepDelay AS INT) AS dep_delay",
)
(typed.write.mode("overwrite").partitionBy("year").parquet(f"{root}/flights"))
print("Parquet snapshot written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lazy transformations and an action
# MAGIC Building the filtered DataFrame defines the query. The count requests a result and triggers execution. The two timings represent different operations and are not a speedup benchmark.

# COMMAND ----------

start = perf_counter()
flights = spark.read.parquet(f"{root}/flights")
selected = (
    flights.filter(F.col("year").between(2005, 2008))
    .filter(F.col("origin").isin("ATL", "ORD", "DFW"))
    .withColumn("observed_delay", F.when(F.col("cancelled") == 0, F.col("dep_delay")))
    .withColumn("delayed_15", (F.col("observed_delay") >= 15).cast("int"))
)
print(f"Read metadata and build transformations: {perf_counter() - start:.3f} s")
start = perf_counter()
flight_count = selected.count()
print(f"Count action: {flight_count:,} scheduled departures in {perf_counter() - start:.3f} s")

# COMMAND ----------

monthly = selected.groupBy("year", "month", "origin").agg(
    F.count("*").alias("scheduled"),
    F.sum("cancelled").alias("cancellations"),
    F.count("observed_delay").alias("observed_departures"),
    F.sum("observed_delay").alias("delay_minutes"),
    F.sum("delayed_15").alias("delayed_departures"),
)
monthly.explain("formatted")
output = f"{root}/airport_monthly"
start = perf_counter()
monthly.write.mode("overwrite").parquet(output)
saved = spark.read.parquet(output)
totals = saved.agg(F.count("*").alias("airport_months"), F.sum("scheduled").alias("scheduled")).first()
assert totals.scheduled == flight_count
assert totals.airport_months == 144
print(f"Wrote and verified {totals.airport_months} airport-months covering {totals.scheduled:,} flights.")
print(f"Write and validation: {perf_counter() - start:.3f} s")
print(output)
saved.createOrReplaceTempView("airport_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL 1. How do cancellation and delay rates compare across airports?
# MAGIC Rates use pooled counts. Cancellation uses all scheduled departures, while delay metrics use noncancelled departures with a recorded departure delay.

# COMMAND ----------

airport_summary = spark.sql("""
    SELECT origin,
           SUM(scheduled) AS scheduled,
           ROUND(100.0 * SUM(cancellations) / SUM(scheduled), 2) AS cancellation_pct,
           ROUND(SUM(delay_minutes) / SUM(observed_departures), 2) AS avg_dep_delay_min,
           ROUND(100.0 * SUM(delayed_departures) / SUM(observed_departures), 2) AS delayed_15_pct
    FROM airport_monthly
    GROUP BY origin
    ORDER BY delayed_15_pct DESC, origin
""")
airport_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL 2. Which month had the highest cancellation rate at each airport?

# COMMAND ----------

worst_months = spark.sql("""
    WITH ranked AS (
        SELECT origin, year, month, scheduled, cancellations,
               100.0 * cancellations / scheduled AS cancellation_pct,
               ROW_NUMBER() OVER (
                   PARTITION BY origin ORDER BY cancellations / scheduled DESC, year, month
               ) AS cancellation_rank
        FROM airport_monthly
    )
    SELECT origin, year, month, scheduled, cancellations,
           ROUND(cancellation_pct, 2) AS cancellation_pct
    FROM ranked
    WHERE cancellation_rank = 1
    ORDER BY origin
""")
worst_months.show(truncate=False)


# COMMAND ----------

# Verify the size of the source files used by this pipeline.
files = [
    item
    for item in dbutils.fs.ls("/databricks-datasets/asa/airlines/")
    if item.name.endswith(".csv")
    and (item.name == "1999.csv" or item.name.startswith("200"))
]
total_bytes = sum(item.size for item in files)
for item in sorted(files, key=lambda item: item.name):
    print(f"{item.name}: {item.size:,} bytes")
print(f"Files: {len(files)}")
print(f"Total: {total_bytes:,} bytes")
print(f"Total: {total_bytes / 1_000_000_000:.3f} GB")
print(f"Total: {total_bytes / 1024**3:.3f} GiB")
assert total_bytes >= 1_000_000_000, "Input is smaller than 1 GB"
print("Verified: source input meets the 1 GB requirement.")
