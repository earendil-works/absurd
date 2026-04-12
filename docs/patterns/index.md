# Patterns

This section collects small, practical patterns for common Absurd use cases.

## Available Patterns

- **[Cron Jobs](./cron.md)** — ensure each cron slot is enqueued exactly once, even with multiple scheduler replicas.
- **[Deploying and Rolling Rollouts](./deployments.md)** — roll out new task types safely without adding hot-path claim overhead.
- **[Living with Code Changes](./living-with-code-changes.md)** — version steps or normalize old checkpoint data when long-lived tasks resume against newer code.
- **[Pi AI Agent](./pi-ai-agent.md)** — persist `message_end` events as durable steps and resume agent loops safely after retries.
