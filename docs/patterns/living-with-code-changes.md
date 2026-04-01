# Living with Code Changes

One of the perks of durable execution is also one of its hazards: your task code
can change while old task state keeps living in Postgres.

If a task sleeps for hours, waits for an event for days, or is simply retried
much later, it may resume against code that did not exist when the task first
started.

There is no magic fix for this.  The trick is to design your steps so old
checkpoint data can either be:

1. **left behind** with a new step name, or
2. **translated forward** by compatibility code.

In practice, most changes fall into one of those two buckets.

## The Core Rule

A completed step returns its cached value forever.

That means if you change what this step returns:

```typescript
const payment = await ctx.step('process-payment', async () => {
  return { chargeId: 'ch_123' };
});
```

into this:

```typescript
const payment = await ctx.step('process-payment', async () => {
  return {
    chargeId: 'ch_123',
    provider: 'stripe',
    receiptEmail: 'jane@example.com',
  };
});
```

then old tasks may still resume with the **old** value shape.

That is normal.  Durable systems remember the past on purpose.

## Strategy 1: Rename the Step

If the meaning or shape changed in a way that is not safely compatible, the
cleanest move is to version the step name.

```typescript
const payment = await ctx.step('process-payment:v2', async () => {
  return {
    chargeId: (await stripe.charges.create({ amount: params.amount })).id,
    provider: 'stripe',
    receiptEmail: params.email ?? null,
  };
});
```

This makes old tasks keep using `process-payment`, while new tasks use
`process-payment:v2`.

Use this when:

- the return type changed substantially
- side effects changed meaning
- you switched providers or APIs
- old data would be ambiguous or unsafe to reinterpret

## Strategy 2: Normalize Old Results

If the change is compatible enough, keep the step name and normalize the cached
result before using it.

```typescript
type LegacyPayment = string;

type PaymentV2 = {
  chargeId: string;
  provider: 'stripe';
  receiptEmail: string | null;
};

function normalizePayment(value: LegacyPayment | PaymentV2): PaymentV2 {
  if (typeof value === 'string') {
    return {
      chargeId: value,
      provider: 'stripe',
      receiptEmail: null,
    };
  }

  return {
    chargeId: value.chargeId,
    provider: value.provider ?? 'stripe',
    receiptEmail: value.receiptEmail ?? null,
  };
}

app.registerTask({ name: 'charge-order' }, async (params, ctx) => {
  const rawPayment = await ctx.step('process-payment', async () => {
    const charge = await stripe.charges.create({ amount: params.amount });
    return {
      chargeId: charge.id,
      provider: 'stripe' as const,
      receiptEmail: params.email ?? null,
    };
  });

  const payment = normalizePayment(rawPayment);

  await ctx.step('send-receipt:v2', async () => {
    if (!payment.receiptEmail) {
      return { skipped: true };
    }

    await sendReceipt(payment.receiptEmail, payment.chargeId);
    return { skipped: false };
  });

  return { payment };
});
```

This is often the best option when you need to tolerate both old and new shapes
during a rollout.

## A Good Practical Pattern

A useful rule of thumb is:

- keep the **compatibility mess** at the boundary
- keep the rest of the task working with the **new normalized shape**

In other words, do the ugly conversion once, right after loading the cached step
result, and let the rest of your task stay clean.

## Other Ideas That Work Well

Some more specific other options.

### Version Step Names Deliberately

Names like these are perfectly reasonable:

- `fetch-user:v2`
- `render-email#2026-04`
- `create-invoice:stripe`

You do not need a global convention.  You just need names that make future-you
understand where the old state ends and the new state begins.

### Prefer Additive Changes

Adding a field is usually easier than changing the meaning of an old one.

Safer:

```typescript
return { chargeId, provider: 'stripe' };
```

Riskier:

```typescript
return { id: chargeId };
```

The first lets old readers ignore `provider`.  The second may break every caller.

### Major Rewrites Become New Tasks

If the whole workflow changed shape, a new step name may not be enough.  In that
case, register a new task name and let old tasks finish on the old code path.

For example:

- `order-fulfillment`
- `order-fulfillment-v2`

That is often simpler than teaching one task to understand too many historical
variants.

### Keep Side Effects Separate

If you split side effects into smaller, clearly named steps, it becomes much
safer to version only the changed part.

For example, changing email rendering should not force you to rename payment
processing too.

### Be Cautious With long-lived Workflows

A task that sleeps forever and wakes up every day accumulates more compatibility
risk than a task that does one job and finishes.

When possible, prefer spawning fresh tasks for recurring work.  That naturally
limits how much ancient checkpoint data has to survive code evolution.

## When in Doubt

If you are unsure whether a change is compatible, assume it is not and rename
the step.

It is usually cheaper to carry one old checkpoint forever than to debug a task
that resumed six months later with a shape your new code no longer understands.
