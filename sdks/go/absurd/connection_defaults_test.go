package absurd

import "testing"

func TestResolveDatabaseURLPrefersExplicitOption(t *testing.T) {
	t.Setenv("ABSURD_DATABASE_URL", "postgresql://localhost/from-absurd")
	t.Setenv("PGDATABASE", "postgresql://localhost/from-pgdatabase")

	got := resolveDatabaseURL("postgresql://localhost/from-options")
	if got != "postgresql://localhost/from-options" {
		t.Fatalf("expected explicit options DatabaseURL to win, got %q", got)
	}
}

func TestResolveDatabaseURLPrefersAbsurdEnvOverPGDatabase(t *testing.T) {
	t.Setenv("ABSURD_DATABASE_URL", "postgresql://localhost/from-absurd")
	t.Setenv("PGDATABASE", "postgresql://localhost/from-pgdatabase")

	got := resolveDatabaseURL("")
	if got != "postgresql://localhost/from-absurd" {
		t.Fatalf("expected ABSURD_DATABASE_URL to win, got %q", got)
	}
}

func TestResolveDatabaseURLUsesPGDatabase(t *testing.T) {
	t.Setenv("ABSURD_DATABASE_URL", "")
	t.Setenv("PGDATABASE", "postgresql://localhost/from-pgdatabase")

	got := resolveDatabaseURL("")
	if got != "postgresql://localhost/from-pgdatabase" {
		t.Fatalf("expected PGDATABASE URL to be used, got %q", got)
	}
}

func TestResolveDatabaseURLNormalizesPGDatabaseName(t *testing.T) {
	t.Setenv("ABSURD_DATABASE_URL", "")
	t.Setenv("PGDATABASE", "absurd")

	got := resolveDatabaseURL("")
	if got != "dbname=absurd" {
		t.Fatalf("expected PGDATABASE name to normalize to dbname=..., got %q", got)
	}
}

func TestResolveDatabaseURLFallsBackToLocalDefault(t *testing.T) {
	t.Setenv("ABSURD_DATABASE_URL", "")
	t.Setenv("PGDATABASE", "")

	got := resolveDatabaseURL("")
	if got != "postgresql://localhost/absurd" {
		t.Fatalf("expected localhost fallback, got %q", got)
	}
}
