import sys

from absurd_sdk import Absurd

should_await = "--await" in sys.argv
args = [arg for arg in sys.argv[1:] if arg != "--await"]
user_id = args[0] if len(args) > 0 else "alice"
email = args[1] if len(args) > 1 else f"{user_id}@example.com"

app = Absurd(queue_name="default")

spawned = app.spawn(
    "provision-user",
    {
        "user_id": user_id,
        "email": email,
    },
)

print("spawned:", spawned)
print("current snapshot:", app.fetch_task_result(spawned["task_id"]))

if should_await:
    print(f"waiting for completion; emit user-activated:{user_id} on queue default")
    print("final snapshot:", app.await_task_result(spawned["task_id"], timeout=300))

app.close()
