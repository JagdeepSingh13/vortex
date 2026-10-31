# Build image for the C++ engine. Every engine build, test and run happens in here,
# so results do not depend on the host OS or compiler (see ADR 0002).
# Ubuntu 24.04 LTS ships GCC 13, CMake 3.28 and Ninja 1.11.
# Libraries come from Ubuntu's packages, each pinned to an exact version:
#   GoogleTest 1.14   tests (ADR 0008)
#   librdkafka 2.3.0  Kafka client (ADR 0014)
#   simdjson 3.6.4    order.v1 parsing on the consumer thread (ADR 0014)
#   yaml-cpp 0.8.0    reading config/symbols.yaml (ADR 0014)
FROM ubuntu:24.04@sha256:fe81560990fae86dc4743202b2cb4d3df98d0bcf949173cecef15ce1127ef852

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        pkg-config \
        libgtest-dev=1.14.0-1 \
        googletest=1.14.0-1 \
        librdkafka-dev=2.3.0-1build2 \
        libsimdjson-dev=3.6.4-1 \
        libyaml-cpp-dev=0.8.0+dfsg-6build1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
