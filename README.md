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
└──────────┬────────────┘
           │ writes
           ▼
  output/
  ├── event-volume/        [Parquet, partitioned by date]
  ├── balance-trends/      [Delta Lake, partitioned by accountId]
  ├── account-activation/  [Parquet]
  └── dlq-error-rate/      [Parquet, partitioned by date]
```

---

## Tech Stack

| Category       | Technology               |
|----------------|--------------------------|
| Language       | Scala 2.13               |
| Big Data       | Apache Spark 3.5.1       |
| Storage        | Parquet + Delta Lake 3.2 |
| Build          | sbt                      |
| Data Generator | Custom JSONL generator   |


---

## Project Structure

```
src/main/scala/analytics/
├── domain/
│   └── AccountEvent.scala     ← sealed ADT (typed Dataset layer)
├── jobs/
│   ├── EventVolume.scala      ← event counts by date + hour + type
│   ├── BalanceTrends.scala    ← running balance via window functions
│   ├── AccountActivation.scala← time to first activity, percentiles
│   └── DlqErrorRate.scala     ← malformed event rate over time
├── schema/
│   └── EventSchema.scala      ← shared StructType + JSONL reader
├── generator/
│   └── DataGenerator.scala    ← reproducible JSONL seed data
└── Main.scala                 ← entry point, job orchestration
```



## Running Locally

### Prerequisites

- Java 17 (Java 21 has known Spark compatibility issues)
- sbt 1.9.x

Verify: `java -version`

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

```scala
import io.delta.tables.DeltaTable
val dt = DeltaTable.forPath(spark, "output/balance-trends/")
dt.history().show()
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
|2026-05-19|2   |AccountCreated |1         |
|2026-05-19|4   |AccountCreated |1         |
|2026-05-19|6   |AccountCredited|1         |
|2026-05-19|8   |AccountCreated |1         |
|2026-05-19|9   |AccountCredited|1         |
|2026-05-19|10  |AccountCredited|1         |
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

---

## Trade-offs and Limitations

- **No tests yet** — Testcontainers integration tests are the next priority
- **Local filesystem only** — no S3/GCS; intentional for portfolio scope
- **Single-node Spark** — `local[*]` mode; no cluster deployment demonstrated
- **DlqErrorRate denominator** — total events include malformed events, making error rate a slight overestimate; documented known approximation
- **No schema registry** — schema evolution is manual; Avro + Schema Registry is the planned upgrade path
- **Domain model uses String for Spark Encoder compatibility** — UUID and Instant are not natively serialisable by Spark's reflection-based Encoder
---

## Interview Q&A

**Why Spark here instead of continuing with FS2?**
FS2 is optimal for low-latency, event-by-event stream processing with backpressure. Spark SQL is better suited for large-scale batch analytics: columnar storage, distributed query optimisation, and native support for complex aggregations like window functions and percentiles. The platform uses both — different tools for different layers.

**Why window functions instead of a fold per account?**
Folding over sorted events per account requires collecting all data for that account to a single executor — unsafe at scale and loses parallelism. Window functions express the same computation declaratively and execute distributed. Spark's Catalyst optimiser can also push them down efficiently.

**Why Delta Lake on BalanceTrends but Parquet on other jobs?**
BalanceTrends is the richest output — the most likely target for incremental updates and repeated analytical queries. Delta Lake's ACID guarantees and time travel justify the overhead there. The other jobs produce simpler append-friendly outputs where Parquet is sufficient and lighter.

**Why explicit schema over inferSchema?**
inferSchema scans the full dataset on every read, is expensive at scale, and silently accepts schema drift. Explicit schema documents the contract, fails fast on unexpected changes, and reads faster.

**Why left join in DlqErrorRate?**
An inner join silently drops days with zero malformed events, making the output incomplete. A left join preserves all days; zero is a meaningful data point — it means the pipeline was clean that day.

**Why ingestedAt on malformed events?**
A broken event's own timestamp cannot be trusted. The ingestion timestamp is owned and controlled by the pipeline, making it the only reliable temporal anchor for events that failed to parse correctly.

**Why Scala 2.13 and not 3?**
Spark 3.x officially targets Scala 2.13. Using 2.13 avoids cross-compilation overhead and keeps this project directly compatible with the upstream Data Pipeline, which made the same choice for the same reason.