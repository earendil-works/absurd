# Quickstart

This guide walks through a small workflow that shows why Absurd is
useful.  We model a basic signup flow here:

1. create a user record
2. try to send an activation email
3. survive a transient failure with a retry
4. wait for an external activation event
5. return a final result you can inspect or await

With a plain queue, you would usually end up inventing your own retry logic,
status table, wake-up mechanism, and polling API.  In Absurd, you write that as
one task.

The exact examples in this guide live in the repository:

- [TypeScript quickstart examples](https://github.com/earendil-works/absurd/tree/main/sdks/typescript/examples/quickstart)
- [Python quickstart examples](https://github.com/earendil-works/absurd/tree/main/sdks/python/examples/quickstart)

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
export PGDATABASE="postgresql://user:pass@localhost:5432/mydb"
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
(`t_`, `r_`, `c_`, `e_`, `w_` suffixed by queue name).

```bash
absurdctl create-queue default
```

## 3. Write the Worker

The worker registers a `provision-user` task.

The important part is the failure story:

- `create-user-record` is a normal checkpointed step
- `demo-transient-outage` fails **once on purpose** so you can see retries
- on retry, the task replays from checkpoints instead of re-running completed work
- the task then waits for `user-activated:<user_id>` and returns a final result

=== "TypeScript"

    ```typescript
    import { Absurd } from "absurd-sdk";

    type ProvisionUserParams = {
      user_id: string;
      email: string;
    };

    type ActivationEvent = {
      activated_at: string;
    };

    const app = new Absurd({ queueName: "default" });

    app.registerTask<ProvisionUserParams>(
      {
        name: "provision-user",
        defaultMaxAttempts: 5,
      },
      async (params, ctx) => {
        const user = await ctx.step("create-user-record", async () => {
          console.log(`[${ctx.taskID}] creating user record for ${params.user_id}`);
          return {
            user_id: params.user_id,
            email: params.email,
            created_at: new Date().toISOString(),
          };
        });

        // Demo only: fail once after the first checkpoint so the retry behavior is visible.
        const outage = await ctx.beginStep<{ simulated: boolean }>(
          "demo-transient-outage",
        );
        if (!outage.done) {
          console.log(`[${ctx.taskID}] simulating a temporary email provider outage`);
          await ctx.completeStep(outage, { simulated: true });
          throw new Error("temporary email provider outage");
        }

        const delivery = await ctx.step("send-activation-email", async () => {
          console.log(`[${ctx.taskID}] sending activation email to ${user.email}`);
          return {
            sent: true,
            provider: "demo-mail",
            to: user.email,
          };
        });

        console.log(`[${ctx.taskID}] waiting for user-activated:${user.user_id}`);
        const activation = (await ctx.awaitEvent(
          `user-activated:${user.user_id}`,
          {
            timeout: 3600,
          },
        )) as ActivationEvent;

        return {
          user_id: user.user_id,
          email: user.email,
          delivery,
          status: "active",
          activated_at: activation.activated_at,
        };
      },
    );

    console.log("worker listening on queue default");
    await app.startWorker({ concurrency: 4 });
    ```

=== "Python"

    ```python
    from datetime import datetime, timezone

    from absurd_sdk import Absurd

    app = Absurd(queue_name="default")


    @app.register_task("provision-user", default_max_attempts=5)
    def provision_user(params, ctx):
        def create_user_record():
            print(f"[{ctx.task_id}] creating user record for {params['user_id']}")
            return {
                "user_id": params["user_id"],
                "email": params["email"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

        user = ctx.step("create-user-record", create_user_record)

        # Demo only: fail once after the first checkpoint so the retry behavior is visible.
        outage = ctx.begin_step("demo-transient-outage")
        if not outage.done:
            print(f"[{ctx.task_id}] simulating a temporary email provider outage")
            ctx.complete_step(outage, {"simulated": True})
            raise RuntimeError("temporary email provider outage")

        def send_activation_email():
            print(f"[{ctx.task_id}] sending activation email to {user['email']}")
            return {
                "sent": True,
                "provider": "demo-mail",
                "to": user["email"],
            }

        delivery = ctx.step("send-activation-email", send_activation_email)

        print(f"[{ctx.task_id}] waiting for user-activated:{user['user_id']}")
        activation = ctx.await_event(f"user-activated:{user['user_id']}", timeout=3600)

        return {
            "user_id": user["user_id"],
            "email": user["email"],
            "delivery": delivery,
            "status": "active",
            "activated_at": activation["activated_at"],
        }


    print("worker listening on queue default")
    app.start_worker()
    ```

Run one of the repository examples in a terminal:

=== "TypeScript"

    ```bash
    cd sdks/typescript
    npm install
    node examples/quickstart/worker.ts
    ```

=== "Python"

    ```bash
    cd sdks/python
    uv run examples/quickstart/worker.py
    ```

## 4. Spawn a Task

In another terminal, spawn a task.  The spawn result gives you a task ID you can
store, inspect later, or await.

=== "TypeScript"

    ```typescript
    import { Absurd } from "absurd-sdk";

    const shouldAwait = process.argv.includes("--await");
    const args = process.argv.slice(2).filter((arg) => arg !== "--await");
    const userID = args[0] ?? "alice";
    const email = args[1] ?? `${userID}@example.com`;

    const app = new Absurd({ queueName: "default" });

    const spawned = await app.spawn("provision-user", {
      user_id: userID,
      email,
    });

    console.log("spawned:", spawned);
    console.log("current snapshot:", await app.fetchTaskResult(spawned.taskID));

    if (shouldAwait) {
      console.log(
        `waiting for completion; emit user-activated:${userID} on queue default`,
      );
      console.log(
        "final snapshot:",
        await app.awaitTaskResult(spawned.taskID, { timeout: 300 }),
      );
    }

    await app.close();
    ```

=== "Python"

    ```python
    import sys

    from absurd_sdk import Absurd

    should_await = "--await" in sys.argv
    args = [arg for arg in sys.argv[1:] if arg != "--await"]
    user_id = args[0] if len(args) > 0 else "alice"
    email = args[1] if len(args) > 1 else f"{user_id}@example.com"

    app = Absurd(queue_name="default")

    spawned = app.spawn(
        "provision-user",
        {
            "user_id": user_id,
            "email": email,
        },
    )

    print("spawned:", spawned)
    print("current snapshot:", app.fetch_task_result(spawned["task_id"]))

    if should_await:
        print(f"waiting for completion; emit user-activated:{user_id} on queue default")
        print("final snapshot:", app.await_task_result(spawned["task_id"], timeout=300))

    app.close()
    ```

=== "CLI"

    ```bash
    absurdctl spawn-task --queue default provision-user \
      -P user_id=alice \
      -P email=alice@example.com
    ```

Run one of the repository clients:

=== "TypeScript"

    ```bash
    cd sdks/typescript
    node examples/quickstart/client.ts alice alice@example.com
    ```

=== "Python"

    ```bash
    cd sdks/python
    uv run examples/quickstart/client.py alice alice@example.com
    ```

If you want to block until the task finishes, pass `--await` and then emit the
activation event from another terminal.

## 5. Wake the Task Up

The task is now suspended in `awaitEvent()`, waiting for `user-activated:alice`.
Wake it up like this:

```bash
absurdctl emit-event --queue default user-activated:alice \
  -P activated_at=2026-04-02T12:00:00Z
```

At that point the waiting task resumes, returns its final value, and moves to a
terminal state.

## 6. Inspect or Await the Result

Most applications will just keep the returned task ID and continue.  But when
you want synchronous behavior in a script, test, or shell, you can also inspect
or await the task result.

The Python and TypeScript clients use the same basic flow: spawn the task,
fetch the current snapshot, then await the terminal result after you emit the
activation event from another terminal.

=== "Python"

    ```pycon
    >>> from absurd_sdk import Absurd
    >>> app = Absurd(queue_name="default")
    >>> spawned = app.spawn(
    ...     "provision-user",
    ...     {"user_id": "bob", "email": "bob@example.com"},
    ... )
    >>> spawned
    {'task_id': '019...', 'run_id': '019...', 'attempt': 1}
    >>> app.fetch_task_result(spawned["task_id"])
    TaskResultSnapshot(state='pending')
    >>> app.await_task_result(spawned["task_id"], timeout=300)
    TaskResultSnapshot(state='completed', result={'user_id': 'bob', 'email': 'bob@example.com', 'delivery': {'sent': True, 'provider': 'demo-mail', 'to': 'bob@example.com'}, 'status': 'active', 'activated_at': '2026-04-02T12:00:00Z'})
    ```

=== "TypeScript"

    ```typescript
    import { Absurd } from "absurd-sdk";

    const app = new Absurd({ queueName: "default" });

    const spawned = await app.spawn("provision-user", {
      user_id: "bob",
      email: "bob@example.com",
    });

    console.log(spawned);
    console.log(await app.fetchTaskResult(spawned.taskID));
    console.log(await app.awaitTaskResult(spawned.taskID, { timeout: 300 }));

    await app.close();
    ```

## 7. What Just Happened?

This example shows the main Absurd model:

- **Retries are part of the normal flow.**  The demo task intentionally fails
  once after the first checkpoint.
- **Completed steps are not re-run.**  On retry, `create-user-record` is loaded
  from Postgres instead of executing again.
- **Waiting is durable.**  `awaitEvent()` suspends the task without losing
  state.
- **Results are queryable.**  You can fetch the current snapshot or await the
  terminal result from another process.

That is the core pitch of Absurd: write one workflow in straight-line code,
while Postgres keeps the checkpoints, retries, and wake-up state.

## Next Steps

- Read the **[Concepts](./concepts.md)** page to understand the full model
- Read **[Cleanup and Retention](./cleanup.md)** before production so task and event data do not grow forever
- Read the **[Living with Code Changes](./patterns/living-with-code-changes.md)** pattern if your tasks may survive deploys or long sleeps
- Explore the **[TypeScript SDK](./sdk-typescript.md)** or **[Python SDK](./sdk-python.md)** API reference
- Use **[Habitat](./habitat.md)** to monitor tasks in a web dashboard
- Use **[absurdctl](./absurdctl.md)** for advanced queue and task management
