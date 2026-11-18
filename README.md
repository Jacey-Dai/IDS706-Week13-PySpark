# Flight delays at major US airports

A PySpark pipeline analyzing departures from Atlanta (ATL), Chicago O'Hare (ORD), and Dallas/Fort Worth (DFW) during 2005–2008. The notebook was successfully executed in Databricks Free Edition on November 18, 2026. All nine exported cells finished without errors.

## Dataset and processing

The input is the [ASA airline on-time dataset](https://stat-computing.org/dataexpo/2009/), available in Databricks at `/databricks-datasets/asa/airlines/{1999,200*}.csv`. Each row represents a scheduled flight. The [TA lab](https://androidilicious.github.io/databricks-pyspark-session/assignment.html) documents this source. The pipeline loads 1999–2008, selects six columns, casts numeric fields, and writes a Parquet snapshot partitioned by year. A subsequent file-listing validation measured **10 source CSV files totaling 6,286,007,363 bytes (6.286 GB / 5.854 GiB)**, meeting the 1GB+ requirement. The source-size screenshot preserves that subsequent execution. Total source rows were not counted.

Two early filters select 2005–2008 and the three airports. Two `withColumn` transformations identify noncancelled flights with recorded departure delay and delays of at least 15 minutes. A complex aggregation groups by year, month, and airport, computing scheduled flights, cancellations, observed departures, delay minutes, and delayed departures. This produces **144 airport-month rows covering 4,295,088 scheduled flights**. Assertions verify the number of groups and reconcile the saved flight total to the filtered input.

Cancellation rates use all scheduled flights. Mean departure delay and the 15-minute delay rate use noncancelled flights with a recorded departure delay. Negative delay values represent early departures and remain in the mean. SQL 1 calculates pooled airport rates from sums. SQL 2 uses a CTE and `ROW_NUMBER` to identify the highest-cancellation airport-month for each airport.

## Findings

| Airport | Scheduled departures | Cancelled | Mean departure delay, minutes | Delayed at least 15 minutes |
|---|---:|---:|---:|---:|
| ORD | 1,437,608 | 3.69% | 16.72 | 28.06% |
| ATL | 1,670,013 | 1.98% | 13.48 | 25.11% |
| DFW | 1,187,467 | 2.21% | 11.50 | 23.21% |

ATL had the most scheduled departures in this subset. ORD had the highest cancellation rate, mean departure delay, and 15-minute delay rate. Its delay rate exceeded DFW's by 4.85 percentage points.

| Airport | Highest-cancellation month | Scheduled | Cancelled flights | Cancellation rate |
|---|---|---:|---:|---:|
| ATL | January 2005 | 35,282 | 2,356 | 6.68% |
| DFW | March 2008 | 23,479 | 1,656 | 7.05% |
| ORD | February 2008 | 27,972 | 3,364 | 12.03% |

These are descriptive comparisons across the selected period. Weather, carrier mix, schedules, and other possible explanations were not modeled.

## Performance analysis

The exported physical plan confirms year partition pruning through `PartitionFilters: isnotnull(year), year >= 2005, year <= 2008`. The airport condition appears in both `DictionaryFilters` and `RequiredDataFilters` at the Photon Parquet scan. Column pruning reduces the data-file `ReadSchema` to month, origin, cancelled, and dep_delay. The year value comes from partition metadata, and the unused destination column is absent from the scan. Staging typed Parquet enables these scan optimizations for subsequent analysis.

The aggregation plan contains a partial `PhotonGroupingAgg`, a shuffle using `hashpartitioning(year, month, origin, 108)`, and a final aggregation. Local partial aggregation combines records before the exchange, reducing the records that need to move between workers. The plan reports full Photon support and an `AdaptiveSparkPlan` with `isFinalPlan=false`, so it records the initial plan. The shuffle is a potential cost, but its actual byte volume, spill, and task skew require Query Details evidence. The persisted 144-row monthly summary lets both SQL queries operate on a small result without rescanning millions of flight records.

The measured metadata-read and transformation-construction time was **0.048 seconds**, the count action took **3.630 seconds**, and the aggregation write plus read-back validation took **2.762 seconds**. The staging cell's screenshot shows **49 seconds** for the whole cell. These scopes differ and the measurements come from one run. They demonstrate lazy construction followed by triggered execution, without establishing a comparative optimization speedup. The source CSV staging was the longest visible cell. The Query Details screenshot records a 1.584-second wall-clock duration for the aggregation write, 47.92 MB read across four files, 4,295,088 rows read, and 144 rows written (5.74 KB). Aggregated task time was 1.32 seconds. The execution list shows five of five tasks completed. Shuffle byte volume is not visible in the supplied details. No explicit cache benchmark was performed.

## Reproduce

1. Import `Week_13_PySpark.py` into Databricks as a notebook.
2. Attach Serverless compute with sample-dataset access and permission to create a volume in `workspace.default`.
3. Run all cells. Re-running replaces this pipeline's Parquet outputs.
4. Export HTML with outputs.

The snapshot is written to `/Volumes/workspace/default/week13_flight_analysis/flights`. The final result is written to `/Volumes/workspace/default/week13_flight_analysis/airport_monthly`. Large data files remain in the workspace.

## Execution evidence

- [Executed notebook HTML](Week_13_PySpark.html)
- [Full physical plan and write validation](results/cell-5-output.txt)
- [Staging success](screenshots/01-staging.png)
- [Lazy construction and count](screenshots/02-lazy-action.png)
- [Aggregation cell](screenshots/03-aggregation.png)
- [Physical plan](screenshots/04-physical-plan.png)
- [Airport summary](screenshots/05-airport-summary.png)
- [Highest-cancellation months](screenshots/06-worst-months.png)

- [Query execution list](screenshots/07-query-executions.png)
- [Query Details for the aggregation write](screenshots/08-query-details.png)

- [Source size validation](screenshots/09-source-size.png)

The HTML export preserves the original nine-cell run. The source script additionally includes the subsequently executed size check, whose output is preserved in the source-size screenshot.
