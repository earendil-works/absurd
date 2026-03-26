# Quickstart

This guide walks you through setting up Absurd from scratch: installing the
schema, creating a queue, spawning a task, and running a worker.

## Prerequisites

- **PostgreSQL** (14 or later)
- **Node.js** (20+) for the TypeScript SDK, or **Python** (3.11+) for the Python SDK
- **Python 3** for `absurdctl`

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

## 2. Create a Queue

Queues are logical groups of tasks.  Each queue gets its own set of tables
(`t_`, `r_`, `c_`, `e_`, `w_` prefixed by queue name).

```bash
absurdctl create-queue default
```

## 3. Write a Task (TypeScript)

Create a file `hello.ts`:

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
    return 'done';
  });

  console.log('greet result:', result);
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

```bash
npm install absurd-sdk
node --experimental-strip-types hello.ts
```

The worker picks up the task, runs it through the steps, and prints:

```
Hello
World, World!
greet result: done
```

If the process crashes between steps, restart it — the completed step is
replayed from the checkpoint and only the remaining work executes.

## Write a Task (Python)

You can do the same thing with the Python SDK:

```python
from absurd_sdk import Absurd
import os

app = Absurd(
    conn_or_url=os.environ["PGDATABASE"],
    queue_name="default",
)

@app.register_task("hello-world")
def hello_world(params, ctx):
    print("Hello")

    result = ctx.step("greet", lambda: f"World, {params['name']}!")
    print("greet result:", result)

app.start_worker()
```

## Next Steps

- Read the **[Concepts](./concepts.md)** page to understand the full model
- Explore the **[TypeScript SDK](./sdk-typescript.md)** or **[Python SDK](./sdk-python.md)** API reference
- Use **[Habitat](./habitat.md)** to monitor tasks in a web dashboard
- Use **[absurdctl](./absurdctl.md)** for advanced queue and task management
