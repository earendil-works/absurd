#!/usr/bin/env -S uv run --script
"""
Validate PostgreSQL migrations by comparing schema dumps.

This script:
1. Creates a test database and applies base + migration SQL files
2. Dumps the resulting schema
3. Creates another test database and applies the current SQL file
4. Dumps that schema
5. Writes both dumps to files for comparison

Usage:
    ./scripts/validate-psql.py \\
        --base=/path/to/base.sql \\
        --migration=/path/to/migration.sql \\
        --current=./sql/absurd.sql \\
        --migration-dump=migration-schema.out \\
        --current-dump=current-schema.out
"""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "psycopg>=3.2.0",
#     "testcontainers>=4.0.0",
# ]
# ///

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List

import psycopg
from testcontainers.postgres import PostgresContainer


# Set DOCKER_HOST for macOS Docker Desktop if not already set
if "DOCKER_HOST" not in os.environ:
    macos_docker_sock = Path.home() / ".docker" / "run" / "docker.sock"
    if macos_docker_sock.exists():
        os.environ["DOCKER_HOST"] = f"unix://{macos_docker_sock}"


def normalize_dsn(url: str) -> str:
    """Normalize DSN to use postgresql:// scheme."""
    if url.startswith("postgresql+psycopg2://"):
        return "postgresql://" + url.split("://", 1)[1]
    return url


def apply_sql_files(dsn: str, sql_files: List[Path]) -> None:
    """Apply a list of SQL files to the database."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        for sql_file in sql_files:
            print(f"  Applying {sql_file.name}...")
            script = sql_file.read_text()
            conn.execute(script)


def dump_schema(dsn: str) -> str:
    """Dump the database schema using pg_dump."""
    # Parse DSN to extract connection parameters
    from urllib.parse import urlparse

    parsed = urlparse(dsn)

    # Build pg_dump command
    env = os.environ.copy()
    env["PGPASSWORD"] = parsed.password or ""

    cmd = [
        "pg_dump",
        "-h", parsed.hostname or "localhost",
        "-p", str(parsed.port or 5432),
        "-U", parsed.username or "postgres",
        "-d", parsed.path.lstrip("/") if parsed.path else "test",
        "--schema-only",
        "--no-comments",
        "--no-security-labels",
        "--no-owner",
        "--no-privileges",
    ]

    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    return result.stdout


def main():
    parser = argparse.ArgumentParser(
        description="Validate PostgreSQL migrations by comparing schemas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--base",
        type=Path,
        required=True,
        help="Path to the base SQL file (e.g., previous version)",
    )
    parser.add_argument(
        "--migration",
        type=Path,
        required=True,
        help="Path to the migration SQL file",
    )
    parser.add_argument(
        "--current",
        type=Path,
        required=True,
        help="Path to the current SQL file (e.g., sql/absurd.sql)",
    )
    parser.add_argument(
        "--migration-dump",
        type=Path,
        required=True,
        help="Output file for the migration schema dump",
    )
    parser.add_argument(
        "--current-dump",
        type=Path,
        required=True,
        help="Output file for the current schema dump",
    )

    args = parser.parse_args()

    # Validate input files exist
    for sql_file in [args.base, args.migration, args.current]:
        if not sql_file.exists():
            print(f"Error: {sql_file} does not exist", file=sys.stderr)
            return 1

    print("Starting PostgreSQL validation...")
    print()

    # Create containers
    print("Starting PostgreSQL containers...")
    migration_container = PostgresContainer("postgres:16-alpine")
    current_container = PostgresContainer("postgres:16-alpine")

    try:
        migration_container.start()
        current_container.start()

        migration_dsn = normalize_dsn(migration_container.get_connection_url())
        current_dsn = normalize_dsn(current_container.get_connection_url())

        # Apply base + migration
        print()
        print("Applying base + migration SQL files...")
        apply_sql_files(migration_dsn, [args.base, args.migration])

        # Apply current
        print()
        print("Applying current SQL file...")
        apply_sql_files(current_dsn, [args.current])

        # Dump schemas
        print()
        print("Dumping migration schema...")
        migration_schema = dump_schema(migration_dsn)

        print("Dumping current schema...")
        current_schema = dump_schema(current_dsn)

        # Write dumps to files
        print()
        print(f"Writing migration schema to {args.migration_dump}...")
        args.migration_dump.write_text(migration_schema)

        print(f"Writing current schema to {args.current_dump}...")
        args.current_dump.write_text(current_schema)

        print()
        print("Done! Schema dumps have been written.")
        print()
        print("To compare the schemas, run:")
        print(f"  diff {args.migration_dump} {args.current_dump}")
        print()

        return 0

    except subprocess.CalledProcessError as e:
        print(f"Error running pg_dump: {e}", file=sys.stderr)
        print(f"stdout: {e.stdout}", file=sys.stderr)
        print(f"stderr: {e.stderr}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        print("Stopping containers...")
        migration_container.stop()
        current_container.stop()


if __name__ == "__main__":
    sys.exit(main())
