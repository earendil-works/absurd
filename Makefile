.PHONY: format test test-core test-typescript test-python test-rust

# Format all code
format:
	@cd sdks/typescript && npx prettier -w .
	@cd habitat/ui && npx prettier -w .
	@uvx ruff format tests
	@gofmt -w habitat
	@cd sdks/rust && cargo fmt

# Run all tests
test: test-core test-typescript test-python test-rust

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

# Run Rust SDK checks and tests
test-rust:
	@echo "Running Rust SDK checks and tests"
	@cd sdks/rust cargo clippy -- -D warnings && cargo test