# Source SQL files in order
SQL_SOURCES := $(sort $(wildcard sql/[0-9][0-9]_*.sql))

.PHONY: format build

# Build the combined absurd.sql from source files
build: sql/absurd.sql

sql/absurd.sql: $(SQL_SOURCES)
	{ \
		printf -- '-- AUTO-GENERATED FILE. Created by running `make build`; manual changes will be overwritten.\n\n'; \
		cat $(SQL_SOURCES); \
	} > $@
	pg_format -s 2 --keyword-case 1 --type-case 1 --function-case 1 --no-extra-line -i $@

# Format all individual SQL source files
format:
	pg_format -s 2 --keyword-case 1 --type-case 1 --function-case 1 --no-extra-line -i $(SQL_SOURCES)
