# Flight delays at major US airports

This PySpark pipeline uses the [ASA airline on-time dataset distributed with Databricks](https://androidilicious.github.io/databricks-pyspark-session/assignment.html). Each source row represents a scheduled domestic flight. The 1999–2008 input spans roughly 64.8 million records in the TA's reference run. The pipeline stages these years in Parquet and analyzes ATL, ORD, and DFW for 2005–2008.

Import `Week_13_PySpark.py` as a Python notebook in Databricks and attach compute with access to the sample dataset and permission to create a volume in `workspace.default`. The notebook writes the staged data and the final airport-month results under `/Volumes/workspace/default/week13_flight_analysis`. Re-running replaces those two pipeline outputs.

## Performance analysis

The staging step reads CSV and converts the required fields to numeric types once. The analytical query filters by year and origin before aggregation. Year is a physical partition column, so the expected plan lists the year range under `PartitionFilters`. The origin condition can appear under `PushedFilters` at the Parquet scan, and `ReadSchema` shows which data columns Spark needs for the aggregation. Confirm those entries in the runtime plan.

Grouping by airport, year, and month requires matching keys across input partitions. A partial aggregate can combine records within each partition before an exchange, reducing the number of records moved. The staging write may also redistribute rows to lay out the year partitions. The SQL queries reuse the small persisted airport-month table, avoiding repeated scans of the flight snapshot. Inspect Query Details for scan time, shuffle size, and any spill before identifying a measured bottleneck.

## Results and execution evidence

Pending a Databricks run. SQL 1 compares pooled delay and cancellation rates for the three airports. SQL 2 reports the highest-cancellation month at each airport. The notebook checks that the saved monthly counts reconcile to the filtered input and that all 144 airport-month combinations are present.
