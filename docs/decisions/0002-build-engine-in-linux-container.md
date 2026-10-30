# 0002. Build the engine in a pinned Linux container

- Status: accepted
- Date: 2026-10-30
- Milestone: M0

## Context

The development machine is Windows 11 with MinGW g++ 15. From M5 the engine links librdkafka, which is awkward to build with MinGW. Benchmarks should also run on the same OS family as the rest of the stack (Linux containers).

## Options

1. **Docker build container**: `infra/docker/engine-build.Dockerfile`, base image pinned by digest; `make engine` runs CMake inside it. Same result on any host with Docker. Slower first build; needs Docker running.
2. **WSL Ubuntu**: native Linux toolchain, good for perf tools. Depends on how each machine's WSL is set up; not reproducible from a clean clone.
3. **Native MinGW**: what `cpp_exchange` used. Fast to start, but librdkafka and Linux-only tooling become a problem in M5.

## Decision

Option 1. Reproducible and matches docker-compose. WSL remains an option for profiling later (separate ADR if needed).

## Consequences

- Engine builds land in `build/engine-release` (gitignored).
- Base image: `ubuntu:24.04` pinned by digest → GCC 13.3, CMake 3.28.3, Ninja 1.11.1. apt packages are not individually version-pinned; the digest plus the Ubuntu LTS archive keeps them stable enough. Revisit if a build ever changes underneath us.
- Benchmark results must state they ran inside Docker Desktop's WSL2 VM, which affects timing.
- The Windows-only TCP tools still build in the container because `net/socket.h` has a POSIX branch.
