# Source SQL files in order
SQL_SOURCES := $(sort $(wildcard sql/[0-9][0-9]_*.sql))

.PHONY: format build

# Build the combined absurd.sql from source files
build: sql/absurd.sql

sql/absurd.sql: $(SQL_SOURCES)
	cat $(SQL_SOURCES) > sql/absurd.sql
	pg_format -s 2 --keyword-case 1 --type-case 1 --function-case 1 -i sql/absurd.sql

# Format all individual SQL source files
format:
	pg_format -s 2 --keyword-case 1 --type-case 1 --function-case 1 -i $(SQL_SOURCES)
