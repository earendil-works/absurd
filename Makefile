.PHONY: format test test-core test-typescript test-python test-go build-absurdctl build-absurdctl-pypi docs serve-docs

# Format all code
format:
	@cd sdks/typescript && npx prettier -w .
	@cd habitat/ui && npx prettier -w .
	@uvx ruff format tests
	@gofmt -w habitat
	@gofmt -w sdks/go/absurd

ZENSICAL_VERSION ?= 0.0.21

# Build documentation site
docs:
	@uvx --from "zensical==$(ZENSICAL_VERSION)" zensical build
	@touch site/.nojekyll

# Serve documentation locally with live reload
serve-docs:
	@uvx --from "zensical==$(ZENSICAL_VERSION)" zensical serve

# Build bundled absurdctl artifacts with embedded schema + migrations
build-absurdctl:
	@./scripts/build-absurdctl

# Build the PyPI staging directory for absurdctl
build-absurdctl-pypi:
	@./scripts/build-absurdctl --pypi-only

# Run all tests
test: test-core test-typescript test-python test-go

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

# Run Go SDK tests
test-go:
	@echo "Running Go SDK tests (root module)"
	@cd sdks/go/absurd && go test ./...
	@echo "Running Go SDK tests (integration module)"
	@cd sdks/go/absurd/absurdtest && go test ./...
