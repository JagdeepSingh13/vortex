# Single entry point for the project. Targets are added by the milestone that
# first needs them; keep the Commands list in CLAUDE.md in sync.

SHELL := bash

ENGINE_IMAGE := vortex/engine-build:m0
ENGINE_BUILD_DIR := build/engine-release

# Git Bash on Windows rewrites paths like /src into C:/Program Files/Git/src
# before they reach docker.exe; this turns that off.
export MSYS_NO_PATHCONV := 1

DOCKER_RUN := docker run --rm -v "$(CURDIR):/src" -w /src $(ENGINE_IMAGE)

.PHONY: help engine-image engine test clean

help: ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

engine-image: ## build the pinned Linux image used to compile the engine
	docker build -t $(ENGINE_IMAGE) -f infra/docker/engine-build.Dockerfile infra/docker

engine: engine-image ## build the engine in Release mode
	$(DOCKER_RUN) bash -c "cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -S engine -B $(ENGINE_BUILD_DIR) && cmake --build $(ENGINE_BUILD_DIR)"

test: engine ## run all unit tests
	$(DOCKER_RUN) ctest --test-dir $(ENGINE_BUILD_DIR) --output-on-failure

clean: ## remove build output
	rm -rf build
