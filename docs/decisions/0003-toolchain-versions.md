# 0003. Toolchain versions

- Status: accepted
- Date: 2026-10-30
- Milestone: M0

## Context

`CLAUDE.md` originally said Java 21 and Python 3.11+. The machine has Java 25 and Python 3.10. Jagdeep asked to use what is available or what is best.

## Options

1. **Follow the original text**: install Java 21 and Python 3.11.
2. **Use what is installed**: Java 25, Python 3.10.
3. **Mix**: keep installed Java 25 (an LTS release supported by current Spring Boot); use Python 3.12 inside containers and local venvs, because Python 3.10 reaches end of life in October 2026.

## Decision

Option 3.

| Tool | Version | Where |
|------|---------|-------|
| C++ | C++20, GCC 13.3 | engine build container (ADR 0002) |
| CMake / Ninja | 3.28.3 / 1.11.1 | engine build container |
| Java | 25 (LTS) | local JDK, `api/` from M8 |
| Python | 3.12 | containers and venvs, from M4 |
| Docker | 28.0.4 | host |

Library and image versions (Kafka, Spark, Iceberg, dbt, Airflow, Spring Boot) are pinned exactly in the milestone that first uses them, after checking compatibility (Iceberg Spark runtime must match Spark minor + Scala version).

## Consequences

- Install Python 3.12 before M4 (e.g. `scoop install python312`), or run the simulator in a container.
- `CLAUDE.md` tech stack updated to match.
