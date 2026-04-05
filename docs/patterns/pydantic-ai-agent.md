# Pydantic AI Agent with Durable Execution

[Pydantic AI](https://ai.pydantic.dev/) is a Python agent framework that
provides type-safe, structured agent interactions with tool use.  This pattern
shows how to run a Pydantic AI agent inside an Absurd task so that every LLM
interaction is durably checkpointed.

The core idea is simple: wrap `agent.run_sync()` (or `await agent.run()`) in an
Absurd **step**.  If the worker crashes after the step completes, the cached
result is returned on retry — no redundant LLM call, no duplicated side effects.

## Why not use DBOS?

Pydantic AI ships a first-party `DBOSAgent` wrapper for
[DBOS](https://docs.dbos.dev/) durable execution.  DBOS is a good system, but
it is a separate runtime with its own database and orchestrator.  If you are
already running Absurd (with Postgres as the single source of truth), adding
DBOS introduces another moving part.

This pattern achieves the same outcome — durable, retryable agent runs — using
only Absurd and Postgres.

## Install

```bash
uv add absurd-sdk pydantic-ai openai
```

Set `OPENAI_API_KEY`, or point `OPENAI_BASE_URL` at a local
OpenAI-compatible model server.

## Define the agent

```python
from dataclasses import dataclass

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext
from pydantic_ai.providers.openai import OpenAIProvider


@dataclass
class TicketDeps:
    db: dict[str, dict]


class TriageResult(BaseModel):
    ticket_id: str
    priority: str  # low | medium | high | critical
    category: str
    summary: str
    suggested_assignee: str


provider = OpenAIProvider()  # reads env vars

triage_agent = Agent(
    "openai:gpt-4o-mini",
    provider=provider,
    output_type=TriageResult,
    instructions=(
        "You are a support-ticket triage agent. "
        "Use the lookup_ticket tool to fetch ticket details, then return a "
        "structured triage result."
    ),
    deps_type=TicketDeps,
)


@triage_agent.tool
def lookup_ticket(ctx: RunContext[TicketDeps], ticket_id: str) -> dict:
    """Fetch a ticket from the database."""
    ticket = ctx.deps.db.get(ticket_id)
    if ticket is None:
        return {"error": f"ticket {ticket_id} not found"}
    return ticket
```

The agent uses a tool to look up ticket data, then returns a structured
`TriageResult`.  Pydantic AI validates the output automatically.

## Wire it into an Absurd task

```python
from absurd_sdk import Absurd

app = Absurd(queue_name="default")


@app.register_task("triage-ticket")
def triage_ticket(params, ctx):
    ticket_id = params["ticket_id"]
    deps = TicketDeps(db=TICKETS)

    # Durable step — cached on completion, skipped on retry.
    def run_agent():
        result = triage_agent.run_sync(
            f"Triage ticket {ticket_id}", deps=deps
        )
        return result.output.model_dump()

    triage = ctx.step("triage", run_agent)

    def persist():
        save_to_db(triage)
        return {"persisted": True, **triage}

    return ctx.step("persist", persist)
```

Each `ctx.step()` call is a checkpoint.  The `"triage"` step runs the full
Pydantic AI agent loop (LLM calls, tool invocations, structured output
validation).  If the worker crashes *after* the step completes, the
serialized result is replayed from Postgres on the next attempt.

## Spawn and await

```python
spawned = app.spawn("triage-ticket", {"ticket_id": "TICK-1001"})
result = app.await_task_result(spawned["task_id"], timeout=120)
print(result.result)  # {"persisted": True, "ticket_id": "TICK-1001", ...}
```

## Async variant

The same pattern works with `AsyncAbsurd` and `agent.run()`:

```python
from absurd_sdk import AsyncAbsurd

app = AsyncAbsurd(queue_name="default")


@app.register_task("triage-ticket")
async def triage_ticket(params, ctx):
    ticket_id = params["ticket_id"]
    deps = TicketDeps(db=TICKETS)

    async def run_agent():
        result = await triage_agent.run(
            f"Triage ticket {ticket_id}", deps=deps
        )
        return result.output.model_dump()

    triage = await ctx.step("triage", run_agent)

    async def persist():
        await save_to_db(triage)
        return {"persisted": True, **triage}

    return await ctx.step("persist", persist)
```

## Multi-step agent workflows

For workflows that chain multiple agent calls, wrap each in its own step:

```python
@app.register_task("research-and-write")
def research_and_write(params, ctx):
    topic = params["topic"]

    # Step 1 — research agent gathers facts
    def research():
        result = research_agent.run_sync(f"Research: {topic}")
        return result.output.model_dump()

    facts = ctx.step("research", research)

    # Step 2 — writer agent drafts the report
    def write():
        result = writer_agent.run_sync(
            f"Write a report on '{topic}' using these facts: {facts}"
        )
        return result.output.model_dump()

    report = ctx.step("write", write)
    return report
```

If the worker crashes between steps, completed agent calls are not repeated.

## Running the example

Full working code lives in
[`sdks/python/examples/pydantic-ai-agent/`](https://github.com/earendil-works/absurd/tree/main/sdks/python/examples/pydantic-ai-agent).

```bash
# Terminal 1 — start the worker
uv run python worker.py

# Terminal 2 — spawn a task and wait
uv run python client.py TICK-1001 --await
```
