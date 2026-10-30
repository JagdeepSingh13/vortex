# 0007. Contract tests with pytest and jsonschema in a pinned container

- Status: accepted
- Date: 2026-10-31
- Milestone: M1

## Context

The schemas need automated tests: valid examples pass, invalid ones fail for the stated reason, and `config/symbols.yaml` stays consistent. The machine has Python 3.10 (end of life October 2026); ADR 0003 chose Python 3.12.

## Options

1. **pytest + jsonschema in a pinned `python:3.12-slim` container.** Same pattern as the engine build (ADR 0002); nothing to install locally; the same language the simulator and Spark jobs will use.
2. **Install Python 3.12 locally.** Faster iteration and better editor support, but setup differs per machine.
3. **Node + ajv in a container.** ajv is a very complete validator, but adds Node to a stack that otherwise has none.

## Decision

Option 1. `make test-schemas` builds `vortex/python-tools` from `infra/docker/python-tools.Dockerfile`; `make test` runs it together with the engine tests.

## Consequences

- Image pinned by digest; all packages, including transitive ones, pinned in `infra/docker/python-tools.requirements.txt`.
- **Strict integers.** The JSON Schema spec counts `18990.0` as an integer. Our validator overrides the integer type to accept only real JSON integers, enforcing "never floats". Other tools (Spark, Java) must be equally strict when they read events; noted for M5/M6/M8.
- Rules JSON Schema cannot express (symbol must exist in `symbols.yaml`; iceberg display ≤ quantity) live in code: the registry check in `schemas/tests/contracts.py`, the iceberg check in the engine.
