# Scala Spark Analytics

A batch analytics layer built on Apache Spark, consuming the partitioned JSONL output of the [Scala Data Pipeline](#) and producing Parquet and Delta Lake datasets for downstream consumption.

Part of a broader event-driven platform: [Account Service](#) · [Notifier Service](#) · [Data Pipeline](#) · **Spark Analytics**

---

## Architecture

```text
┌───────────────────────┐
│   Scala Data Pipeline │
│   FS2 + Kafka         │
│   JSONL persistence   │
└──────────┬────────────┘
           │ writes
           ▼
  data/events-*.jsonl
           │
           ▼
┌───────────────────────┐
│  Scala Spark Analytics│
│                       │
│  explicit schema read │
│  EventVolume job      │
│  BalanceTrends job    │
│  AccountActivation job│
│  DlqErrorRate job     │
│  OverdraftAnalysis job│
│  Parquet + Delta Lake │
└──────────┬────────────┘
           │ writes
           ▼
  output/
  ├── event-volume/        [Parquet, partitioned by date]
  ├── balance-trends/      [Delta Lake, partitioned by accountId]
  ├── account-activation/  [Parquet]
  ├── dlq-error-rate/      [Parquet, partitioned by date]
  └── overdraft-analysis/  [Delta Lake]
```

---

## Tech Stack

| Category       | Technology               |
|----------------|--------------------------|
| Language       | Scala 2.13               |
| Big Data       | Apache Spark 3.5.1       |
| Storage        | Parquet + Delta Lake 3.2 |
| Build          | sbt 1.9.9                |
| Runtime        | Java 17                  |
| Data Generator | Custom JSONL generator   |


---

## Project Structure

```
src/main/scala/analytics/
├── jobs/
│   ├── EventVolume.scala      ← event counts by date + hour + type
│   ├── BalanceTrends.scala    ← running balance via window functions
│   ├── AccountActivation.scala← time to first activity, percentiles
│   ├── DlqErrorRate.scala        ← malformed event rate over time
│   └── OverdraftAnalysis.scala   ← overdraft detection, typed Dataset layer
├── schema/
│   └── EventSchema.scala      ← shared StructType + JSONL reader
├── generator/
│   └── DataGenerator.scala    ← reproducible JSONL seed data
└── Main.scala                 ← entry point, job orchestration
```

---

## Analytics Jobs

### EventVolume
Counts events grouped by `date`, `hour`, and `eventType`. Answers: when is the
platform most active? Useful for capacity planning and detecting unusual traffic
spikes. Malformed and unknown event types are excluded before aggregation.

Output: Parquet, partitioned by `date`.

### BalanceTrends
Reconstructs the running account balance for every account over time using Spark
window functions. Starts from `initialBalance` on `AccountCreated`, applies debits
(negative delta) and credits (positive delta) in chronological order per account.

Key technique: `Window.partitionBy("accountId").orderBy("ts").rowsBetween(unboundedPreceding, currentRow)`
— a cumulative sum scoped per account, safe at scale.

Output: Delta Lake, partitioned by `accountId`.

### AccountActivation
Measures the time between account creation and first activity (debit or credit).
Reports median, p90, and p99 activation latency across all accounts. Accounts
created but never activated are preserved in the output with null `firstActivityAt`
— they contribute to activation rate analysis but are excluded from latency
percentiles.

Output: Parquet.

### DlqErrorRate
Compares daily malformed event volume against total valid event volume to produce
a daily error rate percentage. Malformed events are identified by unknown
`eventType`, null `accountId`, or null `ts`. Events without `occurredAt` are dated
by `ingestedAt` — the timestamp the pipeline received them — since their own
timestamp cannot be trusted when the payload is broken.

Output: Parquet, partitioned by `date`.

### OverdraftAnalysis
Identifies accounts that crossed the zero balance threshold. Uses a typed
`Dataset[OverdraftRow]` for initial filtering and typed `reduceGroups` for
per-account aggregation — demonstrating Spark's Dataset API alongside the
DataFrame API used in other jobs.

Computes per account: first overdraft moment, deepest negative balance, duration
from last positive balance to overdraft, and whether the account subsequently
recovered.

Input: BalanceTrends Delta Lake output.
Output: Parquet.

---

## Key Design Decisions

### Explicit Schema over inferSchema

```scala
val accountEvent = StructType(Seq(
  StructField("eventType", StringType, nullable = true),
  StructField("accountId", StringType, nullable = true),
  ...
))
```

`inferSchema` scans the entire dataset on every read to guess types — expensive
at scale and fragile on schema drift. Explicit schema fails fast on unexpected
shape and documents the contract at the code level.

All fields are `nullable = true` — Spark does not enforce nullability on JSON
reads regardless of schema declaration. Null handling is the responsibility of
each consuming job.

---

### Window Functions for Running Balance

```scala
val windowSpec = Window
  .partitionBy("accountId")
  .orderBy("ts")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

allMovements.withColumn("runningBalance", sum("delta").over(windowSpec))
```

The alternative — folding over sorted events per account — requires collecting
all data for that account to a single executor. Unsafe at scale and loses
parallelism. Window functions express the intent declaratively and execute
distributed across the cluster.

Removing `partitionBy` would produce one global running sum across all accounts
— incorrect. Removing `rowsBetween` would produce the total sum of the entire
partition at every row when two events share an exact timestamp — also incorrect.

---

### Delta Lake for BalanceTrends, Parquet elsewhere

BalanceTrends is the analytically richest output — the most likely target for
repeated queries and incremental updates as new events arrive. Delta Lake adds
ACID transactions, schema enforcement, and time travel on top of Parquet at
minimal additional cost.

The other jobs produce append-friendly, read-once outputs where Delta's overhead
is not justified. Mixing both formats in the same project demonstrates deliberate
choice rather than default.

```scala
// time travel — query what balance trends looked like at first write
spark.read.format("delta")
  .option("versionAsOf", "0")
  .load("output/balance-trends/")
```

---

### Typed Dataset in OverdraftAnalysis

```scala
val overdraftEntries: Dataset[OverdraftRow] = events
  .filter(col("runningBalance") < 0)
  .as[OverdraftRow]

val firstOverdraft = overdraftEntries
  .groupByKey(_.accountId)(Encoders.STRING)
  .reduceGroups { (a, b) =>
    if (a.runningBalance.compareTo(b.runningBalance) < 0) a else b
  }
```

The typed layer uses `groupByKey` and `reduceGroups` — operations that work on
strongly-typed `OverdraftRow` instances with full compiler support. The boundary
between typed Dataset and DataFrame is explicit: typed ingestion and aggregation,
then `.toDF()` before joins and final projection. This mirrors the most common
real-world pattern — typed where domain logic is rich, DataFrame where SQL
expressiveness wins.

`java.math.BigDecimal` is used instead of Scala's `BigDecimal` — Spark's
reflection-based Encoder does not support Scala `BigDecimal` natively.
`java.sql.Date` is replaced with `String` — it triggers Java module access
errors (`sun.util.calendar.ZoneInfo`) on Java 17 inside Spark's Encoder.

---

### Left Join in DlqErrorRate

```scala
totalByDay.join(dlqByDay, Seq("date"), "left")
```

An inner join would silently drop days with zero DLQ events — making the error
rate appear undefined rather than 0.0% on clean days. The left join preserves
all days from `totalByDay`. `coalesce(col("dlqEvents"), lit(0))` converts the
resulting nulls to zero — zero is a meaningful data point.

---

### ingestedAt on Malformed Events

Malformed events carry `ingestedAt` instead of `occurredAt`. A pipeline that
receives a broken event cannot trust the event's own timestamp — it may be
absent, malformed, or spoofed. The ingestion timestamp is always available and
always trustworthy. `EventSchema.read` uses `coalesce(occurredAt, ingestedAt)`
so both valid and malformed events produce a usable `ts` column.

---

### DataGenerator — Causal Ordering

`createdEpoch` is captured as a `val` once per account and used as the strict
lower bound for all subsequent activity timestamps. 

---


### Generate Sample Data

```bash
sbt "runMain analytics.generator.DataGenerator"
```

Produces `data/events-sample.jsonl` — 50 accounts, ~600 events, 7-day window, ~1–4% malformed rate.

### Run All Analytics Jobs

```bash
sbt run
```

Output written to `output/`.

### Inspect Delta Table History

```bash
# from sbt console or Main — shows all versioned writes to BalanceTrends
spark.read.format("delta")
  .option("versionAsOf", "0")
  .load("output/balance-trends/")
  .show(5, truncate = false)
```

---

## Sample Output

### BalanceTrends
```
+-----------+-------------------+---------------+-------+--------------+
|accountId  |ts                 |eventType      |delta  |runningBalance|
+-----------+-------------------+---------------+-------+--------------+
|account-002|2026-05-20 16:44:17|AccountCreated |125.00 |125.00        |
|account-002|2026-05-20 23:08:19|AccountCredited|224.00 |349.00        |
|account-002|2026-05-22 07:16:08|AccountDebited |-430.00|-81.00        |
+-----------+-------------------+---------------+-------+--------------+
```

### EventVolume
```
+----------+----+---------------+----------+
|date      |hour|eventType      |eventCount|
+----------+----+---------------+----------+
|2026-05-19|20  |AccountDebited |1         |
|2026-05-19|20  |AccountCreated |2         |
|2026-05-19|21  |AccountCreated |4         |
|2026-05-19|22  |AccountCreated |4         |
+----------+----+---------------+----------+

```

### AccountActivation
```
+------------------+-----------------+-----------------+
|median_hours      |p90_hours        |p99_hours        |
+------------------+-----------------+-----------------+
|6.01              |23.23            |47.41            |
+------------------+-----------------+-----------------+
```

### DlqErrorRate
```
+----------+-----------+---------+------------+
|date      |totalEvents|dlqEvents|errorRatePct|
+----------+-----------+---------+------------+
|2026-05-23|90         |1        |1.11        |
|2026-05-24|138        |5        |3.62        |
|2026-05-25|239        |10       |4.18        |
+----------+-----------+---------+------------+
```

### OverdraftAnalysis
```
+-----------+-------------------+-------------------+------------------+----------------+-------------+---------+
|accountId  |lastPositiveAt     |overdraftStartedAt |maxOverdraftDepth |hoursInOverdraft|latestBalance|recovered|
+-----------+-------------------+-------------------+------------------+----------------+-------------+---------+
|account-011|2026-05-22 10:40:11|2026-05-23 16:46:52|-1569.00          |30.11           |-1219.00     |false    |
|account-034|2026-05-21 21:16:57|2026-05-23 23:09:26|-606.00           |49.87           |-68.00       |false    |
|account-008|2026-05-25 19:46:04|2026-05-22 16:24:51|-342.00           |12.43           |575.00       |true     |
+-----------+-------------------+-------------------+------------------+----------------+-------------+---------+
```

---

## Trade-offs and Limitations

- **No tests yet** — Testcontainers integration tests are the next priority
- **Local filesystem only** — no S3/GCS; intentional for portfolio scope
- **Single-node Spark** — `local[*]` mode; no cluster deployment demonstrated
- **DlqErrorRate denominator** — total events include malformed events, making error rate a slight overestimate; documented known approximation
- **No schema registry** — schema evolution is manual; Avro + Schema Registry is the planned upgrade path
- **Spark Encoder type constraints** — domain model uses `java.math.BigDecimal` (not Scala `BigDecimal`) and `String` (not `java.sql.Date`) due to Spark Encoder limitations on Java 17; documented in OverdraftRow
- **DataGenerator is local only** — will be extracted to a separate continuous Kafka producer service in a future iteration
---

## Interview Q&A

**Why Spark here instead of continuing with FS2?**
FS2 is optimal for low-latency, event-by-event stream processing with backpressure. Spark SQL is better suited for large-scale batch analytics: columnar storage, distributed query optimisation, and native support for complex aggregations like window functions and percentiles. The platform uses both — different tools for different layers.

**Why window functions instead of a fold per account?**
Folding over sorted events per account requires collecting all data for that account to a single executor — unsafe at scale and loses parallelism. Window functions express the same computation declaratively and execute distributed. Spark's Catalyst optimiser can also push them down efficiently.

**Why Delta Lake on BalanceTrends but Parquet on other jobs?**
BalanceTrends is the richest output — the most likely target for incremental
updates and repeated analytical queries. Delta Lake's ACID guarantees and time
travel justify the overhead. The other jobs produce simpler append-friendly
outputs where Parquet is sufficient and lighter.

**What are ACID guarantees and why do they matter here?**
Atomicity: a write either fully succeeds or fully fails — no partial Parquet files
on crash. Consistency: schema enforcement rejects incompatible writes. Isolation:
concurrent readers always see a complete version, never a mix of old and new files.
Durability: committed writes survive crashes via the `_delta_log/` transaction log.
Plain Parquet has none of these — a crashed overwrite leaves a corrupted directory.

**Why typed Dataset in OverdraftAnalysis but DataFrame everywhere else?**
DataFrame is more concise for SQL-expressible aggregations and joins. The typed
Dataset layer earns its complexity when domain logic benefits from compiler
enforcement — `groupByKey(_.accountId)` and `reduceGroups` on `OverdraftRow`
operate on typed instances with full IDE support. The boundary is explicit:
typed for filtering and per-key aggregation, DataFrame for joins and projection.

**Why explicit schema over inferSchema?**
inferSchema scans the full dataset on every read, is expensive at scale, and silently accepts schema drift. Explicit schema documents the contract, fails fast on unexpected changes, and reads faster.

**Why left join in DlqErrorRate?**
An inner join silently drops days with zero malformed events, making the output incomplete. A left join preserves all days; zero is a meaningful data point — it means the pipeline was clean that day.

**Why ingestedAt on malformed events?**
A broken event's own timestamp cannot be trusted. The ingestion timestamp is owned and controlled by the pipeline, making it the only reliable temporal anchor for events that failed to parse correctly.

**Why Scala 2.13 and not 3?**
Spark 3.x officially targets Scala 2.13. Using 2.13 avoids cross-compilation
overhead and keeps this project directly compatible with the upstream Data Pipeline.
The rest of the platform (Account Service, Notifier) uses Scala 3 — each project
uses the version appropriate to its runtime constraints.

**Why java.math.BigDecimal and String instead of Scala types in OverdraftRow?**
Spark's reflection-based Encoder does not support Scala `BigDecimal` natively —
it requires `java.math.BigDecimal`. `java.sql.Date` triggers an
`IllegalAccessError` on Java 17 (`sun.util.calendar.ZoneInfo` is not exported
by the Java module system to unnamed modules). `String` is the safe alternative
since the date column is only used for partitioning, not arithmetic.