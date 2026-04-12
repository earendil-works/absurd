# Deploying and Rolling Rollouts

Absurd keeps claim and execution hot paths intentionally simple.

In particular, workers do not send a per-claim capability filter (for example,
"only claim these task names").  This avoids adding overhead and complexity to
normal steady-state processing for an edge case that should be rare in healthy
operations.

## What Happens During a Rollout

If a worker claims a task name it does not have registered yet, SDK workers
(**TypeScript**, **Python**, and **Go**) will:

1. **defer** the claimed run via `absurd.schedule_run(...)` (not fail it), and
2. put it back to sleeping for a short delay (with jitter), then
3. let another poll cycle pick it up later.

This prevents burning attempts just because a rollout is mid-flight.

> Queue mismatch (`task registered for a different queue`) is still treated as
> misconfiguration and fails immediately.

## Recommended Rollout Strategy

Use a two-phase rollout whenever you introduce new task names:

1. **Deploy workers first**
   - New code is running and can execute the new task handlers.
2. **Enable producers second**
   - Start spawning the new task names only after workers are live.

If producer and worker live in the same binary, gate task production behind a
feature flag/config toggle and enable it after the worker rollout has converged.

## Why This Design

- Keeps claim path fast and simple for everyone.
- Handles rollout race windows safely.
- Pushes complexity to the exceptional path instead of every claim.
- The same rules must be followed for SQL DDL changes and similar systems.

## Operational Notes

- If no capable worker ever appears, deferred tasks will keep cycling in
  sleeping/pending states.
- For hard upper bounds on these loops, use cancellation policy with
  `max_duration` (evaluated after first start). `max_delay` only applies before
  a task is first started.
- Monitor task states during deploys (Habitat / `absurdctl list-tasks`) to
  confirm old workers are draining and new task types are being consumed.
