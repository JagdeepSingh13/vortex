# Learning guides

Plain-language explanations of how this project works and why, written so you can explain every part in an interview. Each guide ends with **check yourself** questions and **go deeper** pointers.

Read in this order:

| # | Guide | What you will understand |
|---|-------|--------------------------|
| 1 | [Docker and the build container](01-docker-build-container.md) | What is inside the container, how it differs from a VM, what every `docker run` flag does |
| 2 | [Make, CMake and Ninja](02-make-cmake-ninja.md) | What happens when you type `make engine`, why builds are incremental, Release vs Debug, warnings |
| 3 | [Running the engine](03-running-the-engine.md) | `run-sim` vs `run-exchange`, the threads and queues inside, how to read the dashboard, Ctrl+C |
| 4 | [Git subtree and history](04-git-subtree-and-history.md) | How the old engine repo became `engine/` with its history, subtree vs submodule |
| 5 | [Event contracts](05-event-contracts.md) | Ticks and lots, JSON Schema line by line, the float and 2^53 traps, trade identity and dedup |
| 6 | [Benchmarking and latency](06-benchmarking-and-latency.md) | Percentiles vs averages, TSC timing, warm-up, reading our real results, OS noise, coordinated omission |
| 7 | [Testing the engine](07-testing-the-engine.md) | Golden replay as the safety net for optimisation, sanitizers, the span lifetime trap |
| 8 | [Kafka basics](08-kafka-basics.md) | Logs, partitions and keys, consumer groups and lag, KRaft, listeners, our measured throughput and latency |
| 9 | [The order simulator](09-order-simulator.md) | Modelling makers and noise traders, idempotent producers, async produce, open-loop pacing, the CRC32 vs murmur2 partitioner bug |

Also see:
- [MILESTONES.md](../MILESTONES.md) — what each milestone (M0–M11) is for
- [decisions/](../decisions/) — the formal record of each design choice (ADRs)
- [INTERVIEW_NOTES.md](../INTERVIEW_NOTES.md) — short talking points per milestone

New guides are added whenever a milestone introduces something worth learning (Kafka, Iceberg, dbt, ...).
