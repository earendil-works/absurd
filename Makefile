.PHONY: format test test-core test-typescript

# Format TypeScript files only
format:
	@cd sdks/typescript && npx prettier -w .
	@cd habitat/ui && npx prettier -w .
	@uvx ruff format tests

# Run all tests
test: test-core test-typescript

# Run core tests
test-core:
	@echo "Running core tests"
	@cd tests; uv run pytest

# Run TypeScript SDK tests
test-typescript:
	@echo "Running TypeScript SDK tests"
	@cd sdks/typescript && npm run test
