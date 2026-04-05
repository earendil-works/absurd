# Patterns

This section collects small, practical patterns for common Absurd use cases.

## Available Patterns

- **[Cron Jobs With Deduplication Keys](./cron.md)** — ensure each cron slot is enqueued exactly once, even with multiple scheduler replicas.
- **[Living with Code Changes](./living-with-code-changes.md)** — version steps or normalize old checkpoint data when long-lived tasks resume against newer code.
- **[Pi AI Agent Durable Turns](./pi-ai-agent.md)** — persist `message_end` events as durable steps and resume agent loops safely after retries.
- **[Pydantic AI Agent](./pydantic-ai-agent.md)** — run a Pydantic AI agent inside a durable Absurd task with checkpointed LLM calls.
