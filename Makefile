.PHONY: format test test-core test-typescript test-python build-absurdctl docs serve-docs

# Format all code
format:
	@cd sdks/typescript && npx prettier -w .
	@cd habitat/ui && npx prettier -w .
	@uvx ruff format tests
	@gofmt -w habitat

ZENSICAL_VERSION ?= 0.0.21

# Build documentation site
docs:
	@uvx --from "zensical==$(ZENSICAL_VERSION)" zensical build
	@touch site/.nojekyll

# Serve documentation locally with live reload
serve-docs:
	@uvx --from "zensical==$(ZENSICAL_VERSION)" zensical serve

# Build bundled absurdctl with embedded schema + migrations
build-absurdctl:
	@./scripts/build-absurdctl

# Run all tests
test: test-core test-typescript test-python

# Run core tests
test-core:
	@echo "Running core tests"
	@cd tests; uv run pytest

# Run TypeScript SDK checks and tests
test-typescript:
	@echo "Running TypeScript SDK checks and tests"
	@cd sdks/typescript && npm run type-check && npm run test

# Run Python SDK tests
test-python:
	@echo "Running Python SDK tests"
	@cd sdks/python; uv run pytest
