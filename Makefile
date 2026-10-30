# Single entry point for the project. Targets are added by the milestone that
# first needs them; keep the Commands list in CLAUDE.md in sync.

SHELL := bash

ENGINE_IMAGE := vortex/engine-build:m2
ENGINE_BUILD_DIR := build/engine-release
PYTHON_IMAGE := vortex/python-tools:m1

# Git Bash on Windows rewrites paths like /src into C:/Program Files/Git/src
# before they reach docker.exe; this turns that off.
export MSYS_NO_PATHCONV := 1

DOCKER_RUN := docker run --rm -v "$(CURDIR):/src" -w /src $(ENGINE_IMAGE)

# interactive runs: -it for keyboard input and Ctrl+C, --init so signals reach the program
ENGINE_CONTAINER := vortex-exchange
ENGINE_PORT := 9000
DOCKER_RUN_IT := docker run --rm -it --init -v "$(CURDIR):/src" -w /src
DOCKER_EXEC_IT := docker exec -it $(ENGINE_CONTAINER)

.PHONY: help engine-image engine python-image test test-engine test-schemas clean run-sim run-exchange run-bots run-trader

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

engine-image: ## build the pinned Linux image used to compile the engine
	docker build -t $(ENGINE_IMAGE) -f infra/docker/engine-build.Dockerfile infra/docker

engine: engine-image ## build the engine in Release mode
	$(DOCKER_RUN) bash -c "cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -S engine -B $(ENGINE_BUILD_DIR) && cmake --build $(ENGINE_BUILD_DIR)"

python-image: ## build the pinned Python image used for contract tests
	docker build -t $(PYTHON_IMAGE) -f infra/docker/python-tools.Dockerfile infra/docker

test: test-engine test-schemas ## run all unit tests

test-engine: engine ## run the engine's C++ tests
	$(DOCKER_RUN) ctest --test-dir $(ENGINE_BUILD_DIR) --output-on-failure

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
