.PHONY: format test test-python test-typescript

# Format TypeScript files only
format:
	@cd sdks/typescript && npx prettier -w .
	@cd habitat/ui && npx prettier -w .
	@uvx ruff format tests

# Run all tests
test: test-python test-typescript

# Run Python tests
test-python:
	@cd tests; uv run pytest

# Run TypeScript SDK tests
test-typescript:
	@cd sdks/typescript && npm test
