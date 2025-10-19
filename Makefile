.PHONY: format

format:
	pg_format -s 2 --keyword-case 1 --type-case 1 --function-case 1 -i sql/*.sql
