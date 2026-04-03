# SDKs

Absurd currently has first-party SDKs for:

- **[TypeScript](./typescript.md)** (also usable from JavaScript)
- **[Python](./python.md)**
- **[Go](./go.md)** *(experimental)*

Absurd SDKs are intentionally lightweight. Most durable-execution complexity
lives in the Postgres schema and stored procedures, while SDKs provide
language-idiomatic APIs for tasks, steps, retries, sleeps, and events.

For the underlying model and design rationale, see:

- **[Concepts](../concepts.md)**
- **[Database Setup and Migrations](../database.md)**

If you want an SDK for another language, please open an issue in the
**[issue tracker](https://github.com/earendil-works/absurd/issues)**.
