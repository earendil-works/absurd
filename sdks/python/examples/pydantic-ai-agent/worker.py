"""Pydantic AI agent with durable execution powered by Absurd.

Each agent interaction is wrapped in an Absurd step, so progress survives
crashes and retries without repeating completed LLM calls.

Requires:
    uv add absurd-sdk pydantic-ai openai

Set OPENAI_API_KEY (or point OPENAI_BASE_URL at a local model).

Run:
    uv run python worker.py
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from openai import OpenAI
from pydantic import BaseModel

from pydantic_ai import Agent, RunContext
from pydantic_ai.providers.openai import OpenAIProvider

from absurd_sdk import Absurd

# ---------------------------------------------------------------------------
# 1. Define the Pydantic AI agent
# ---------------------------------------------------------------------------


@dataclass
class TicketDeps:
    """Dependencies available to agent tools."""

    db: dict[str, dict]  # fake ticket store keyed by ticket_id


class TriageResult(BaseModel):
    """Structured output the agent must return."""

    ticket_id: str
    priority: str  # "low" | "medium" | "high" | "critical"
    category: str
    summary: str
    suggested_assignee: str


provider = OpenAIProvider()  # reads OPENAI_API_KEY / OPENAI_BASE_URL from env

triage_agent = Agent(
    "openai:gpt-4o-mini",
    provider=provider,
    output_type=TriageResult,
    instructions=(
        "You are a support-ticket triage agent. "
        "Use the lookup_ticket tool to fetch ticket details, then return a "
        "structured triage result with priority, category, summary, and "
        "suggested assignee."
    ),
    deps_type=TicketDeps,
)


@triage_agent.tool
def lookup_ticket(ctx: RunContext[TicketDeps], ticket_id: str) -> dict:
    """Fetch the full ticket record from the database.

    Args:
        ctx: The run context carrying dependencies.
        ticket_id: The ticket identifier to look up.
    """
    ticket = ctx.deps.db.get(ticket_id)
    if ticket is None:
        return {"error": f"ticket {ticket_id} not found"}
    return ticket


# ---------------------------------------------------------------------------
# 2. Wire the agent into an Absurd durable task
# ---------------------------------------------------------------------------

app = Absurd(queue_name="default")

# Simulated ticket database
TICKETS: dict[str, dict] = {
    "TICK-1001": {
        "id": "TICK-1001",
        "subject": "Cannot log in after password reset",
        "body": (
            "I reset my password 10 minutes ago and I still get "
            "'invalid credentials' when I try to sign in."
        ),
        "reporter": "alice@example.com",
        "created": "2025-06-01T09:12:00Z",
    },
    "TICK-1002": {
        "id": "TICK-1002",
        "subject": "Feature request: dark mode",
        "body": "It would be great to have a dark theme option in the settings.",
        "reporter": "bob@example.com",
        "created": "2025-06-01T10:30:00Z",
    },
}


@app.register_task("triage-ticket")
def triage_ticket(params, ctx):
    ticket_id = params["ticket_id"]
    deps = TicketDeps(db=TICKETS)

    # ---- durable step: run the agent ----
    # If this step already completed on a previous attempt, the cached result
    # is returned immediately — no redundant LLM call.
    def run_agent():
        result = triage_agent.run_sync(
            f"Triage ticket {ticket_id}", deps=deps
        )
        return result.output.model_dump()

    triage = ctx.step("triage", run_agent)

    # ---- durable step: persist the triage decision ----
    def persist_triage():
        print(f"[{ctx.task_id}] persisted triage for {ticket_id}: {json.dumps(triage)}")
        return {"persisted": True, **triage}

    result = ctx.step("persist", persist_triage)
    return result


print("worker listening on queue default — triage-ticket")
app.start_worker()
