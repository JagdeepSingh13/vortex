# Single entry point for the project. Targets are added by the milestone that
# first needs them; keep the Commands list in CLAUDE.md in sync.

SHELL := bash

ENGINE_IMAGE := vortex/engine-build:m5
ENGINE_BUILD_DIR := build/engine-release
PYTHON_IMAGE := vortex/python-tools:m4

# Git Bash on Windows rewrites paths like /src into C:/Program Files/Git/src
# before they reach docker.exe; this turns that off.
export MSYS_NO_PATHCONV := 1

COMMA := ,

DOCKER_RUN := docker run --rm -v "$(CURDIR):/src" -w /src $(ENGINE_IMAGE)

# benchmark: result file named after the UTC date and commit (+ -dirty if uncommitted changes)
BENCH_REQUESTS ?= 2000000
BENCH_ENV ?= Docker Desktop (WSL2 VM) on Windows 11
BENCH_CPU ?= 2
BENCH_COMMIT := $(shell git rev-parse --short HEAD)$(shell git diff --quiet HEAD -- engine || echo -dirty)
BENCH_FILE = benchmarks/results/$(shell date -u +%F)-$(BENCH_COMMIT).md

# local infrastructure (ADR 0012, 0016). Credentials come from .env (gitignored);
# `make up` creates it from .env.example on a fresh clone.
COMPOSE := docker compose --env-file .env -f infra/docker/docker-compose.yml
INFRA_SERVICES := kafka seaweedfs iceberg-rest
WAREHOUSE_BUCKET := warehouse
# -T: no terminal, for scripted commands; interactive ones (tail, send) keep the terminal
KAFKA_BIN := $(COMPOSE) exec -T kafka /opt/kafka/bin
KAFKA_BIN_IT := $(COMPOSE) exec kafka /opt/kafka/bin
BOOTSTRAP := --bootstrap-server localhost:9092
TOPICS := orders trades
TOPIC_PARTITIONS := 3
TOPIC_RETENTION_MS := 604800000
TOPIC ?= orders

# order simulator (ADR 0013): joins the compose network to reach kafka:29092
KAFKA_NETWORK := vortex_default
RATE ?= 1000
DURATION ?= 0
COUNT ?= 0
SEED ?= 42
# -it only when there is a terminal (Ctrl+C works interactively; scripts and CI have none)
TTY_FLAGS := $(shell [ -t 0 ] && echo -it)
SIM_RUN = docker run --rm --network $(KAFKA_NETWORK) -e PYTHONPATH=/src/simulator \
	-e COMMIT=$(shell git rev-parse --short HEAD) -e "BENCH_ENV=$(BENCH_ENV)" -v "$(CURDIR):/src" -w /src

# sanitizer builds: optimised but with debug info, only the test binary
SANITIZER_FLAGS := -DCMAKE_BUILD_TYPE=RelWithDebInfo
SANITIZER_ENV := -e ASAN_OPTIONS=halt_on_error=1:detect_leaks=1 \
	-e UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
	-e TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1

# $(1) build dir, $(2) sanitizers
define sanitizer_test
	$(DOCKER_RUN) bash -c "cmake -G Ninja $(SANITIZER_FLAGS) -DVORTEX_SANITIZE=$(2) -S engine -B $(1) && cmake --build $(1) --target engine_tests"
	docker run --rm $(SANITIZER_ENV) -v "$(CURDIR):/src" -w /src $(ENGINE_IMAGE) ctest --test-dir $(1) --output-on-failure
endef

.PHONY: sim sim-bench test-sim engine-run trades-check e2e-bench stream-bronze bronze-check bronze-restart-test bronze-bench
.PHONY: up up-ui down reset topics kafka-topics kafka-tail kafka-send kafka-groups kafka-offsets kafka-check kafka-perf
.PHONY: help engine-image engine bench python-image test test-all test-engine test-asan test-tsan test-schemas clean

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

engine-image: ## build the pinned Linux image used to compile the engine
	docker build -t $(ENGINE_IMAGE) -f infra/docker/engine-build.Dockerfile infra/docker

engine: engine-image ## build the engine in Release mode
	$(DOCKER_RUN) bash -c "cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -S engine -B $(ENGINE_BUILD_DIR) && cmake --build $(ENGINE_BUILD_DIR)"

bench: engine ## run the engine benchmark and write a dated result to benchmarks/results/
	docker run --rm -e COMMIT=$(BENCH_COMMIT) -e "BENCH_ENV=$(BENCH_ENV)" -e BENCH_CPU=$(BENCH_CPU) \
		-v "$(CURDIR):/src" -w /src $(ENGINE_IMAGE) \
		taskset -c $(BENCH_CPU) $(ENGINE_BUILD_DIR)/bench/latency_bench $(BENCH_REQUESTS) $(BENCH_FILE)

python-image: ## build the pinned Python image used for contract tests
	docker build -t $(PYTHON_IMAGE) -f infra/docker/python-tools.Dockerfile infra/docker

test: test-engine test-schemas test-sim ## run all tests (engine, contracts, simulator)

test-all: test test-asan test-tsan ## everything, plus the slow sanitizer builds

test-engine: engine ## run the engine's C++ tests
	$(DOCKER_RUN) ctest --test-dir $(ENGINE_BUILD_DIR) --output-on-failure

test-asan: engine-image ## engine tests under AddressSanitizer + UndefinedBehaviorSanitizer
	$(call sanitizer_test,build/engine-asan,address$(COMMA)undefined)

test-tsan: engine-image ## engine tests under ThreadSanitizer (data races)
	$(call sanitizer_test,build/engine-tsan,thread)

test-schemas: python-image ## validate schemas, examples and config/symbols.yaml
	docker run --rm -v "$(CURDIR):/src" -w /src $(PYTHON_IMAGE) python -m pytest schemas/tests -q -p no:cacheprovider

test-sim: python-image ## simulator produces only valid order.v1 messages
	docker run --rm -v "$(CURDIR):/src" -w /src $(PYTHON_IMAGE) python -m pytest simulator/tests -q -p no:cacheprovider

clean: ## remove build output
	rm -rf build

# ---------------- local infrastructure ----------------

.env:
	cp .env.example .env
	@echo "created .env from .env.example (local credentials; gitignored)"

up: .env ## start local infrastructure (Kafka, SeaweedFS, Iceberg catalog) and wait until healthy
	$(COMPOSE) up -d --wait $(INFRA_SERVICES)
	@# Iceberg never creates buckets; making it again is harmless
	@$(COMPOSE) exec -T seaweedfs sh -c "echo 's3.bucket.create -name $(WAREHOUSE_BUCKET)' | weed shell -master=localhost:9333" \
		>/dev/null 2>&1 || true

up-ui: up ## start infrastructure plus Kafka UI on http://localhost:8080 (UI needs ~25 s to start)
	$(COMPOSE) --profile ui up -d --wait

down: .env ## stop local infrastructure (data is kept)
	$(COMPOSE) --profile ui --profile stream down

reset: .env ## stop local infrastructure and delete its data (Kafka, tables and the stream checkpoint together)
	@# every profile, or volumes of profile-only services (the bronze checkpoint) would survive
	$(COMPOSE) --profile ui --profile stream down -v

topics: ## create the Kafka topics (safe to run again)
	@for topic in $(TOPICS); do 		$(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --create --if-not-exists --topic $$topic 			--partitions $(TOPIC_PARTITIONS) --replication-factor 1 			--config retention.ms=$(TOPIC_RETENTION_MS); 	done
	@$(MAKE) --no-print-directory kafka-topics

# ---------------- Kafka CLI shortcuts ----------------

kafka-topics: ## describe topics: partitions, leader, replicas, config
	$(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --describe --topic '$(subst $(eval) ,|,$(TOPICS))'

kafka-tail: ## live messages with key, partition, offset, time (TOPIC=orders, FROM_START=1 for history)
	$(KAFKA_BIN_IT)/kafka-console-consumer.sh $(BOOTSTRAP) --topic $(TOPIC) $(if $(FROM_START),--from-beginning) 		--formatter-property print.timestamp=true --formatter-property print.partition=true 		--formatter-property print.offset=true --formatter-property print.key=true

kafka-send: ## type messages by hand as KEY|VALUE, e.g. AAPL|{...} (TOPIC=orders)
	$(KAFKA_BIN_IT)/kafka-console-producer.sh $(BOOTSTRAP) --topic $(TOPIC) 		--reader-property parse.key=true --reader-property 'key.separator=|'

kafka-groups: ## consumer groups: offsets and lag per partition
	$(KAFKA_BIN)/kafka-consumer-groups.sh $(BOOTSTRAP) --all-groups --describe

kafka-offsets: ## messages written so far: latest offset per partition
	@for topic in $(TOPICS); do $(KAFKA_BIN)/kafka-get-offsets.sh $(BOOTSTRAP) --topic $$topic; done

kafka-check: ## round trip: send example orders, read them back, check keys keep partitions
	bash infra/kafka/check.sh

kafka-perf: ## Kafka producer/consumer benchmark, writes benchmarks/results/<date>-kafka-<commit>.md
	bash infra/kafka/perf.sh benchmarks/results/$(shell date -u +%F)-kafka-$(shell git rev-parse --short HEAD).md

# ---------------- order simulator ----------------

sim: python-image ## send simulated orders to Kafka (RATE=1000/s, DURATION=s, COUNT=n, SEED=42; Ctrl+C stops)
	$(SIM_RUN) $(TTY_FLAGS) --init $(PYTHON_IMAGE) python -m vortex_sim \
		--rate $(RATE) --duration $(DURATION) --count $(COUNT) --seed $(SEED)

sim-bench: python-image ## max simulator send rate, 500k orders, writes benchmarks/results/<date>-sim-<commit>.md
	$(SIM_RUN) $(PYTHON_IMAGE) python -m vortex_sim --rate 0 --count 500000 --seed $(SEED) \
		--report benchmarks/results/$(shell date -u +%F)-sim-$(shell git rev-parse --short HEAD).md

# ---------------- engine on Kafka ----------------

engine-run: engine ## run the engine on Kafka: orders in, trades out (Ctrl+C stops it cleanly)
	docker run --rm $(TTY_FLAGS) --init --name vortex-engine --network $(KAFKA_NETWORK) \
		-v "$(CURDIR):/src" -w /src $(ENGINE_IMAGE) $(ENGINE_BUILD_DIR)/src/vortex_engine

trades-check: python-image ## validate trades on Kafka for DURATION s (default 30): schema, unique keys, order, latency
	$(SIM_RUN) $(PYTHON_IMAGE) python -m vortex_sim.trades_check --duration $(if $(filter 0,$(DURATION)),30,$(DURATION))

e2e-bench: engine python-image ## simulator -> engine -> Kafka latency at several rates, writes benchmarks/results/<date>-e2e-<commit>.md
	ENGINE_IMAGE=$(ENGINE_IMAGE) PYTHON_IMAGE=$(PYTHON_IMAGE) bash infra/e2e/bench.sh \
		benchmarks/results/$(shell date -u +%F)-e2e-$(shell git rev-parse --short HEAD).md

# ---------------- streaming (lakehouse) ----------------

stream-bronze: up ## stream Kafka trades into Iceberg vortex.bronze.trades (Ctrl+C stops; restarts resume)
	$(COMPOSE) --profile stream up --build bronze

# one-off Spark jobs in the same image and network as the stream
SPARK_RUN = $(COMPOSE) --profile stream run --rm --no-deps -T bronze /opt/spark/bin/spark-submit \
	--master local[2] --driver-memory 768m

bronze-check: up ## reconcile Kafka trades with Iceberg bronze: no loss, no duplicates, no gaps
	$(SPARK_RUN) /opt/vortex/streaming/bronze_check.py

bronze-restart-test: engine python-image up ## kill -9 the bronze job twice mid-stream, then reconcile (no loss, no duplicates)
	ENGINE_IMAGE=$(ENGINE_IMAGE) PYTHON_IMAGE=$(PYTHON_IMAGE) bash streaming/restart_test.sh

bronze-bench: engine python-image up ## bronze ingest lag and files per trigger interval and rate, writes benchmarks/results/<date>-bronze-<commit>.md
	ENGINE_IMAGE=$(ENGINE_IMAGE) PYTHON_IMAGE=$(PYTHON_IMAGE) bash streaming/bench.sh \
		benchmarks/results/$(shell date -u +%F)-bronze-$(shell git rev-parse --short HEAD).md
