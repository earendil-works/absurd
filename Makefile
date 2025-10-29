.PHONY: format test

# Format TypeScript files only
format:
	@cd sdks/typescript && npx prettier -w .
	@cd habitat/ui && npx prettier -w .
	@uvx ruff format tests

# Run the tests
test:
	@cd tests; uv run pytest
