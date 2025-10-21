<div style="text-align: center" align="center">
  <img src="logo.jpg" width="350" alt="Une photo d'un éléphant avec le titre : « Ceci n'est pas un éléphant »">
  <br><br>
</div>

# Absurd

Absurd is the most simple durable execution workflow system you can think of.
It's entirely based on Postgres and nothing else.  It's almost as easy to use as
a queue, but it handles scheduling and retries, and it does all of that without
needing any other services to run in addition to Postgres.

*… because it's absurd how much you can over-design such a simple thing.*

## Push vs Pull

Absurd is a pull based system which means that your code pulls tasks from
Postgres as it has capacity.  It does not support push at all, which would
require another service to run.  Push systems have the inherent disadvantage
that you need to take greater care of system load constraints and that's a path
we don't want to go down without going into IaC.  If you need this, you can
write yourself a simple service that pulls from Postgres and invokes tasks.

## Queues and State

Absurd keeps both queues and state in Postgres as you would expect.  The state
is stored according to TTLing policies which permits efficient state expiration
by dropping entire partitions.  Queues are modelled after
[pqgm](https://github.com/pgmq/pgmq).

## Highlevel Operations

Absurd is built on super tiny SDKs that just execute the underlying
stored procedures.  However those SDKs are what makes the system convenient
because it abstracts over the lowlevel operations in a way that makes it
convenient for the language you are working with.

### TypeScript Example

```typescript
import { Absurd } from 'absurd';

const app = new Absurd();

app.registerTask('order-fulfillment', async (params, ctx) => {
  // Each step is checkpointed - if the process crashes, we resume from the last completed step
  const payment = await ctx.step('process-payment', async () => {
    return await stripe.charges.create({ amount: params.amount, ... });
  });

  // ensure we have the inventory reserved which will also trigger
  const inventory = await ctx.step('reserve-inventory', async () => {
    return await db.reserveItems(params.items);
  });

  // Wait indefinitely for a warehouse event - the task suspends until the event arrives
  const shipment = await ctx.awaitEvent('await-shipment', `shipment.packed:${params.orderId}`);

  // ready to send a notification!
  await ctx.step('send-notification', async () => {
    return await sendEmail(params.email, shipment);
  });

  return { orderId: payment.id, trackingNumber: shipment.tracking };
});

// Start a worker that pulls tasks from Postgres
await app.startWorker();
```

Spawn a task:

```typescript
// Spawn a task - it will be executed durably with automatic retries.  If
// triggered from within a task, you can also await it.
app.spawn('order-fulfillment', {
  orderId: '42',
  amount: 9999,
  items: ['widget-1', 'gadget-2'],
  email: 'customer@example.com'
});
```