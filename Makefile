# Source SQL files in order
SQL_SOURCES := $(sort $(wildcard sql/[0-9][0-9]_*.sql))

.PHONY: format build

# Build the combined absurd.sql from source files
build: sql/absurd.sql

sql/absurd.sql: $(SQL_SOURCES)
	@{ \
		printf -- '-- AUTO-GENERATED FILE. Created by running `make build`; manual changes will be overwritten.\n\n'; \
		cat $(SQL_SOURCES); \
	} > $@

# Format TypeScript files only
format:
	@cd sdks/typescript && npx prettier -w .
	@cd habitat/ui && npx prettier -w .
