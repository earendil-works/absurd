# Patterns

This section collects small, practical patterns for common Absurd use cases.

## Available Patterns

- **[Cron Jobs With Deduplication Keys](./cron.md)** — ensure each cron slot is enqueued exactly once, even with multiple scheduler replicas.
- **[Pi AI Agent Durable Turns](./pi-ai-agent.md)** — persist `message_end` events as durable steps and resume agent loops safely after retries.
