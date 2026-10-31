# Milestones in plain language

The project is split into 13 milestones (M0–M11, plus M2.5). Each one is small enough to finish, test and explain on its own, gets its own git branch, and ends with an entry in [INTERVIEW_NOTES.md](INTERVIEW_NOTES.md). The exact acceptance criteria live in [ROADMAP.md](ROADMAP.md); this file explains what each step is for.

## The big picture

```
orders come in -> engine matches them -> trades go out -> trades are stored -> trades become analytics
```

The milestones build that pipeline from left to right, one piece at a time.

## Foundation

### M0 — Repository bootstrap ✅
Set up the empty project: folders, the existing C++ engine imported, a one-command build (`make engine`, `make test`) and the docs. Nothing new is built; this is the workbench.

### M1 — Event contracts and symbol config ✅
Decide exactly what an "order" message and a "trade" message look like: fields, types, integers only. Create `config/symbols.yaml` listing each symbol with its tick size (smallest price step) and lot size (smallest quantity step). Every other part speaks this language, so it is agreed first.

### M2 — Engine tests and benchmark ✅
Make the C++ engine trustworthy before connecting it to anything:
- a benchmark that measures real latency and throughput numbers to quote in interviews
- a golden replay test that proves matching behaviour never changes by accident, plus a multi-threaded test for the lock-free queue
- trades carry real wall-clock time (UTC) next to the engine's deterministic logical clock

### M2.5 — Engine performance optimisation (deferred, optional)
Find out where the engine spends its time (profiling), then make the slowest parts faster, one change at a time. Every change must keep the golden replay test passing unchanged, so the engine does exactly the same thing, just faster, and every claim comes with before/after benchmark numbers. Deferred: the engine already works and is measured, and it is complex enough; this is for later, if there is time.

## Getting data moving

### M3 — Local Kafka ✅
Run Kafka on the laptop with Docker and create two topics (message channels): `orders` and `trades`. Kafka is the pipe connecting every part of the system.

### M4 — Order simulator ✅
A Python script that pretends to be many traders, sending fake but valid orders into `orders`, so the system has steady input.

### M5 — Engine Kafka I/O ✅
Connect the engine to Kafka: read orders from `orders`, match them, publish trades to `trades`. The hardest C++ milestone: the matching thread must stay fast, so it hands trades through a lock-free queue to a separate thread that talks to Kafka.

After M5 there is a **live, running exchange**.

## Storing the data (the lakehouse)

### M6 — Bronze tables ✅
A Spark streaming job copies every trade from Kafka into an Iceberg table, exactly as received ("bronze" = raw). Tests prove no trades are lost or duplicated, even when the job crashes and restarts.

### M8 — Order API, live prices and live candles (done before M7)
A new Java (Spring Boot) service that turns the stream of trades into live market data in Redis: the latest price and the current 1-minute candle per symbol, updated on every trade. A terminal viewer shows the candles moving live. This is the fast "hot path"; the engine only matches orders and never computes prices or candles. Moved before M7 so the system becomes visible sooner. Market data only: accepting orders over HTTP moved to M11.

### M7 — Silver tables
Clean up bronze: remove duplicates, fix types, organise by symbol and day ("silver" = clean and trustworthy). Handles events that arrive late. Together with gold (M9) this is the slower "cold path": authoritative history that can always be rebuilt from bronze.

## Analysing

### M9 — dbt gold models and Airflow
- **dbt** turns silver data into business tables: 1-minute candles (open, high, low, close, volume), VWAP, daily stats. The first place prices become decimals.
- **Airflow** runs this on a schedule, checks data quality and cleans up old Iceberg files.

## Cloud and finishing

### M10 — AWS with Terraform
Move the storage layer to real AWS S3, with infrastructure written as Terraform code. Nothing that costs money is created without explicit approval and a cost estimate first.

### M11 — Dashboard, CI and polish
Charts over the gold tables, GitHub Actions running tests on every push, and the final README, architecture diagram and demo script.

## What each stage lets you say in an interview

| After | Claim |
|---|---|
| M2 | "I built a tested, benchmarked C++ matching engine." |
| M5 | "It runs as a live exchange over Kafka." |
| M7 | "Trades land in an Iceberg lakehouse with no loss or duplicates." |
| M9 | "Analytics are built with dbt and orchestrated with Airflow." |
| M11 | "The whole platform runs end to end, partly on AWS, with CI." |
