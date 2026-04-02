# Quickstart

This guide walks you through setting up Absurd from scratch: installing the
schema, creating a queue, spawning a task, and running a worker.

## Prerequisites

- **PostgreSQL** (14 or later)
- **Node.js** with native TypeScript type stripping for the TypeScript SDK
- **Python** (3.11+) with **`uv`** for the Python SDK
- **`absurdctl`** on your `PATH`

If you are working from a checkout of this repository, the easiest way to make
`absurdctl` available is:

```bash
export PATH="$PWD:$PATH"
```

## 1. Install the Schema

Absurd ships as a single SQL file.  Apply it to any Postgres database:

```bash
# Point absurdctl at your database
export PGDATABASE="postgresql://user:pass@localhost:5432/mydb"

# Initialize the absurd schema
absurdctl init
```

This creates the `absurd` schema with all stored procedures and helper
functions.  If you prefer, you can also apply `sql/absurd.sql` directly with
`psql` or plug it into your migration system.

For production deployments, it is usually better to keep Absurd schema changes
inside your existing database migration flow.  See
**[Database Setup and Migrations](./database.md)** for the recommended workflow,
including how to generate upgrade SQL with `absurdctl migrate --dump-sql`.

## 2. Create a Queue

Queues are logical groups of tasks.  Each queue gets its own set of tables
(`t_`, `r_`, `c_`, `e_`, `w_` prefixed by queue name).

```bash
absurdctl create-queue default
```

## 3. Write a Task (TypeScript)

Create a small Node project and install the SDK plus `pg`:

```bash
mkdir absurd-ts-quickstart
cd absurd-ts-quickstart
npm init -y
npm install absurd-sdk
```

Then create a file `hello.mts`:

```typescript
import { Absurd } from 'absurd-sdk';

const app = new Absurd({
  db: process.env.PGDATABASE,
  queueName: 'default',
});

app.registerTask({ name: 'hello-world' }, async (params, ctx) => {
  console.log('Hello');

  const result = await ctx.step('greet', async () => {
    console.log(`World, ${params.name}!`);
    return { greeting: `World, ${params.name}!` };
  });

  console.log('greet result:', result);
  return result;
});

await app.startWorker();
```

## 4. Spawn the Task

Use `absurdctl` to enqueue a task:

```bash
absurdctl spawn-task --queue default hello-world -P name=World
```

Or spawn it programmatically with the TypeScript SDK:

```typescript
import { Absurd } from 'absurd-sdk';

const app = new Absurd({
  db: process.env.PGDATABASE,
  queueName: 'default',
});

const { taskID } = await app.spawn('hello-world', { name: 'World' });
console.log('Spawned task:', taskID);

await app.close();
```

## 5. Run the Worker

Start the worker in one terminal:

```bash
node hello.mts
```

The worker picks up the task, runs it through the steps, and prints:

```
Hello
World, World!
greet result: { greeting: 'World, World!' }
```

If the process crashes between steps, restart it — the completed step is
replayed from the checkpoint and only the remaining work executes.

## Write a Task (Python)

You can do the same thing with the Python SDK:

```python
# /// script
# dependencies = ["absurd-sdk"]
# ///

from absurd_sdk import Absurd
import os

app = Absurd(
    conn_or_url=os.environ["PGDATABASE"],
    queue_name="default",
)

@app.register_task("hello-world")
def hello_world(params, ctx):
    print("Hello")

    def greet():
        message = f"World, {params['name']}!"
        print(message)
        return {"greeting": message}

    result = ctx.step("greet", greet)
    print("greet result:", result)
    return result

app.start_worker()
```

Save that as `hello.py` and run it with:

```bash
uv run hello.py
```

Then, from another terminal, enqueue the task:

```bash
absurdctl spawn-task --queue default hello-world -P name=World
```

## Next Steps

- Read the **[Concepts](./concepts.md)** page to understand the full model
- Read **[Cleanup and Retention](./cleanup.md)** before production so task and event data do not grow forever
- Read the **[Living with Code Changes](./patterns/living-with-code-changes.md)** pattern if your tasks may survive deploys or long sleeps
- Explore the **[TypeScript SDK](./sdk-typescript.md)** or **[Python SDK](./sdk-python.md)** API reference
- Use **[Habitat](./habitat.md)** to monitor tasks in a web dashboard
- Use **[absurdctl](./absurdctl.md)** for advanced queue and task management
