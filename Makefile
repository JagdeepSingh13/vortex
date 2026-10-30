# Single entry point for the project. Targets are added by the milestone that
# first needs them; keep the Commands list in CLAUDE.md in sync.

SHELL := bash

ENGINE_IMAGE := vortex/engine-build:m2
ENGINE_BUILD_DIR := build/engine-release
PYTHON_IMAGE := vortex/python-tools:m1

# Git Bash on Windows rewrites paths like /src into C:/Program Files/Git/src
# before they reach docker.exe; this turns that off.
export MSYS_NO_PATHCONV := 1

COMMA := ,

DOCKER_RUN := docker run --rm -v "$(CURDIR):/src" -w /src $(ENGINE_IMAGE)

# interactive runs: -it for keyboard input and Ctrl+C, --init so signals reach the program
ENGINE_CONTAINER := vortex-exchange
ENGINE_PORT := 9000
DOCKER_RUN_IT := docker run --rm -it --init -v "$(CURDIR):/src" -w /src
DOCKER_EXEC_IT := docker exec -it $(ENGINE_CONTAINER)

# benchmark: result file named after the UTC date and commit (+ -dirty if uncommitted changes)
BENCH_REQUESTS ?= 2000000
BENCH_ENV ?= Docker Desktop (WSL2 VM) on Windows 11
BENCH_CPU ?= 2
BENCH_COMMIT := $(shell git rev-parse --short HEAD)$(shell git diff --quiet HEAD -- engine || echo -dirty)
BENCH_FILE = benchmarks/results/$(shell date -u +%F)-$(BENCH_COMMIT).md

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

.PHONY: help engine-image engine bench python-image test test-engine test-asan test-tsan test-schemas clean run-sim run-exchange run-bots run-trader

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

test: test-engine test-asan test-tsan test-schemas ## run all unit tests (engine, sanitizers, contracts)

test-engine: engine ## run the engine's C++ tests
	$(DOCKER_RUN) ctest --test-dir $(ENGINE_BUILD_DIR) --output-on-failure

test-asan: engine-image ## engine tests under AddressSanitizer + UndefinedBehaviorSanitizer
	$(call sanitizer_test,build/engine-asan,address$(COMMA)undefined)

test-tsan: engine-image ## engine tests under ThreadSanitizer (data races)
	$(call sanitizer_test,build/engine-tsan,thread)

test-schemas: python-image ## validate schemas, examples and config/symbols.yaml
	docker run --rm -v "$(CURDIR):/src" -w /src $(PYTHON_IMAGE) python -m pytest schemas/tests -q -p no:cacheprovider

run-sim: engine ## engine + bots in one process with a live dashboard (Ctrl+C to stop)
	$(DOCKER_RUN_IT) $(ENGINE_IMAGE) $(ENGINE_BUILD_DIR)/tools/simulation

run-exchange: engine ## exchange server on port 9000 with dashboard (Ctrl+C to stop)
	$(DOCKER_RUN_IT) --name $(ENGINE_CONTAINER) -p $(ENGINE_PORT):$(ENGINE_PORT) $(ENGINE_IMAGE) $(ENGINE_BUILD_DIR)/src/exchange $(ENGINE_PORT)

run-bots: ## market maker + noise trader bots (needs run-exchange in another terminal)
	$(DOCKER_EXEC_IT) $(ENGINE_BUILD_DIR)/tools/liquidity_bot 127.0.0.1 $(ENGINE_PORT)

run-trader: ## interactive trader terminal (needs run-exchange in another terminal)
	$(DOCKER_EXEC_IT) $(ENGINE_BUILD_DIR)/tools/trader_terminal 127.0.0.1 $(ENGINE_PORT)

clean: ## remove build output
	rm -rf build
