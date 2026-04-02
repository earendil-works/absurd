import { describe, test, expect } from "./testlib.ts";
import { Absurd } from "../src/index.ts";

describe("Connection defaults", () => {
  test("prefers ABSURD_DATABASE_URL over PGDATABASE", async () => {
    const oldAbsurdDatabaseUrl = process.env.ABSURD_DATABASE_URL;
    const oldPgDatabase = process.env.PGDATABASE;

    process.env.ABSURD_DATABASE_URL = "postgresql://localhost/absurd";
    process.env.PGDATABASE = "postgresql://localhost/other";

    try {
      const app = new Absurd();
      expect((app as any).con.options.connectionString).toBe(
        "postgresql://localhost/absurd",
      );
      await app.close();
    } finally {
      if (oldAbsurdDatabaseUrl === undefined) {
        delete process.env.ABSURD_DATABASE_URL;
      } else {
        process.env.ABSURD_DATABASE_URL = oldAbsurdDatabaseUrl;
      }
      if (oldPgDatabase === undefined) {
        delete process.env.PGDATABASE;
      } else {
        process.env.PGDATABASE = oldPgDatabase;
      }
    }
  });

  test("falls back to PGDATABASE, then localhost/absurd", async () => {
    const oldAbsurdDatabaseUrl = process.env.ABSURD_DATABASE_URL;
    const oldPgDatabase = process.env.PGDATABASE;

    delete process.env.ABSURD_DATABASE_URL;
    process.env.PGDATABASE = "postgresql://localhost/from-pgdatabase";

    try {
      const fromPgDatabase = new Absurd();
      expect((fromPgDatabase as any).con.options.connectionString).toBe(
        "postgresql://localhost/from-pgdatabase",
      );
      await fromPgDatabase.close();

      delete process.env.PGDATABASE;

      const fallback = new Absurd();
      expect((fallback as any).con.options.connectionString).toBe(
        "postgresql://localhost/absurd",
      );
      await fallback.close();
    } finally {
      if (oldAbsurdDatabaseUrl === undefined) {
        delete process.env.ABSURD_DATABASE_URL;
      } else {
        process.env.ABSURD_DATABASE_URL = oldAbsurdDatabaseUrl;
      }
      if (oldPgDatabase === undefined) {
        delete process.env.PGDATABASE;
      } else {
        process.env.PGDATABASE = oldPgDatabase;
      }
    }
  });
});
