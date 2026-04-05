"""Spawn a triage-ticket task and optionally wait for its result.

Run:
    uv run python client.py TICK-1001
    uv run python client.py TICK-1001 --await
"""

import sys

from absurd_sdk import Absurd

should_await = "--await" in sys.argv
args = [arg for arg in sys.argv[1:] if arg != "--await"]
ticket_id = args[0] if args else "TICK-1001"

app = Absurd(queue_name="default")

spawned = app.spawn("triage-ticket", {"ticket_id": ticket_id})
print("spawned:", spawned)
print("current snapshot:", app.fetch_task_result(spawned["task_id"]))

if should_await:
    print("waiting for triage result …")
    final = app.await_task_result(spawned["task_id"], timeout=120)
    print("final snapshot:", final)

app.close()
