# Build image for the C++ engine. Every engine build and test runs in here,
# so results do not depend on the host OS or compiler (see ADR 0002).
# Ubuntu 24.04 LTS ships GCC 13, CMake 3.28 and Ninja 1.11.
# GoogleTest 1.14 comes from Ubuntu's package, pinned to an exact version (ADR 0008).
# librdkafka is added in M5 when the engine first talks to Kafka.
FROM ubuntu:24.04@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        libgtest-dev=1.14.0-1 \
        googletest=1.14.0-1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
