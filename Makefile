.PHONY: format

# Format TypeScript files only
format:
	@cd sdks/typescript && npx prettier -w .
	@cd habitat/ui && npx prettier -w .
