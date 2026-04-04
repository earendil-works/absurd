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

=== "TypeScript"

    ```typescript
    const payment = await ctx.step('process-payment', async () => {
      return { chargeId: 'ch_123' };
    });
    ```

=== "Python"

    ```python
    def process_payment():
        return {"charge_id": "ch_123"}


    payment = ctx.step("process-payment", process_payment)
    ```

=== "Go"

    ```go
    type PaymentV1 struct {
        ChargeID string `json:"charge_id"`
    }

    payment, err := absurd.Step(
        ctx,
        "process-payment",
        func(ctx context.Context) (PaymentV1, error) {
            return PaymentV1{ChargeID: "ch_123"}, nil
        },
    )
    if err != nil {
        return err
    }
    _ = payment
    ```

into this:

=== "TypeScript"

    ```typescript
    const payment = await ctx.step('process-payment', async () => {
      return {
        chargeId: 'ch_123',
        provider: 'stripe',
        receiptEmail: 'jane@example.com',
      };
    });
    ```

=== "Python"

    ```python
    def process_payment():
        return {
            "charge_id": "ch_123",
            "provider": "stripe",
            "receipt_email": "jane@example.com",
        }


    payment = ctx.step("process-payment", process_payment)
    ```

=== "Go"

    ```go
    type PaymentV2 struct {
        ChargeID     string `json:"charge_id"`
        Provider     string `json:"provider"`
        ReceiptEmail string `json:"receipt_email"`
    }

    payment, err := absurd.Step(
        ctx,
        "process-payment",
        func(ctx context.Context) (PaymentV2, error) {
            return PaymentV2{
                ChargeID:     "ch_123",
                Provider:     "stripe",
                ReceiptEmail: "jane@example.com",
            }, nil
        },
    )
    if err != nil {
        return err
    }
    _ = payment
    ```

then old tasks may still resume with the **old** value shape.

That is normal.  Durable systems remember the past on purpose.

## Strategy 1: Rename the Step

If the meaning or shape changed in a way that is not safely compatible, the
cleanest move is to version the step name.

=== "TypeScript"

    ```typescript
    const payment = await ctx.step('process-payment:v2', async () => {
      return {
        chargeId: `charge-${params.amount}`,
        provider: 'stripe',
        receiptEmail: params.email ?? null,
      };
    });
    ```

=== "Python"

    ```python
    def process_payment_v2():
        return {
            "charge_id": f"charge-{params['amount']}",
            "provider": "stripe",
            "receipt_email": params.get("email"),
        }


    payment = ctx.step("process-payment:v2", process_payment_v2)
    ```

=== "Go"

    ```go
    type PaymentV2 struct {
        ChargeID     string `json:"charge_id"`
        Provider     string `json:"provider"`
        ReceiptEmail string `json:"receipt_email"`
    }

    payment, err := absurd.Step(
        ctx,
        "process-payment:v2",
        func(ctx context.Context) (PaymentV2, error) {
            return PaymentV2{
                ChargeID:     fmt.Sprintf("charge-%d", params.Amount),
                Provider:     "stripe",
                ReceiptEmail: params.Email,
            }, nil
        },
    )
    if err != nil {
        return err
    }
    _ = payment
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

=== "TypeScript"

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
        return {
          chargeId: `charge-${params.amount}`,
          provider: 'stripe' as const,
          receiptEmail: params.email ?? null,
        };
      });

      const payment = normalizePayment(rawPayment);

      await ctx.step('send-receipt:v2', async () => {
        if (!payment.receiptEmail) {
          return { skipped: true };
        }

        return {
          skipped: false,
          sentTo: payment.receiptEmail,
          chargeId: payment.chargeId,
        };
      });

      return { payment };
    });
    ```

=== "Python"

    ```python
    def normalize_payment(value):
        if isinstance(value, str):
            return {
                "charge_id": value,
                "provider": "stripe",
                "receipt_email": None,
            }

        return {
            "charge_id": value["charge_id"],
            "provider": value.get("provider", "stripe"),
            "receipt_email": value.get("receipt_email"),
        }


    @app.register_task(name="charge-order")
    def charge_order(params, ctx):
        def process_payment():
            return {
                "charge_id": f"charge-{params['amount']}",
                "provider": "stripe",
                "receipt_email": params.get("email"),
            }

        raw_payment = ctx.step("process-payment", process_payment)
        payment = normalize_payment(raw_payment)

        def send_receipt_v2():
            if not payment["receipt_email"]:
                return {"skipped": True}

            return {
                "skipped": False,
                "sent_to": payment["receipt_email"],
                "charge_id": payment["charge_id"],
            }

        ctx.step("send-receipt:v2", send_receipt_v2)

        return {"payment": payment}
    ```

=== "Go"

    ```go
    type PaymentV2 struct {
        ChargeID     string `json:"charge_id"`
        Provider     string `json:"provider"`
        ReceiptEmail string `json:"receipt_email"`
    }

    func normalizePayment(value any) PaymentV2 {
        switch value := value.(type) {
        case string:
            return PaymentV2{
                ChargeID:     value,
                Provider:     "stripe",
                ReceiptEmail: "",
            }
        case map[string]any:
            chargeID, _ := value["charge_id"].(string)
            provider, _ := value["provider"].(string)
            if provider == "" {
                provider = "stripe"
            }
            receiptEmail, _ := value["receipt_email"].(string)
            return PaymentV2{
                ChargeID:     chargeID,
                Provider:     provider,
                ReceiptEmail: receiptEmail,
            }
        default:
            panic("unexpected payment shape")
        }
    }

    rawPayment, err := absurd.Step[any](
        ctx,
        "process-payment",
        func(ctx context.Context) (any, error) {
            return map[string]any{
                "charge_id":     fmt.Sprintf("charge-%d", params.Amount),
                "provider":      "stripe",
                "receipt_email": params.Email,
            }, nil
        },
    )
    if err != nil {
        return err
    }

    payment := normalizePayment(rawPayment)

    _, err = absurd.Step(
        ctx,
        "send-receipt:v2",
        func(ctx context.Context) (map[string]any, error) {
            if payment.ReceiptEmail == "" {
                return map[string]any{"skipped": true}, nil
            }

            return map[string]any{
                "skipped":   false,
                "sent_to":   payment.ReceiptEmail,
                "charge_id": payment.ChargeID,
            }, nil
        },
    )
    if err != nil {
        return err
    }
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

=== "TypeScript"

    ```typescript
    return { chargeId, provider: 'stripe' };
    ```

=== "Python"

    ```python
    return {"charge_id": charge_id, "provider": "stripe"}
    ```

=== "Go"

    ```go
    return map[string]any{"charge_id": chargeID, "provider": "stripe"}, nil
    ```

Riskier:

=== "TypeScript"

    ```typescript
    return { id: chargeId };
    ```

=== "Python"

    ```python
    return {"id": charge_id}
    ```

=== "Go"

    ```go
    return map[string]any{"id": chargeID}, nil
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
