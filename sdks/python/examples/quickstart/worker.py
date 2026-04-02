from datetime import datetime, timezone

from absurd_sdk import Absurd

app = Absurd(queue_name="default")


@app.register_task("provision-user", default_max_attempts=5)
def provision_user(params, ctx):
    def create_user_record():
        print(f"[{ctx.task_id}] creating user record for {params['user_id']}")
        return {
            "user_id": params["user_id"],
            "email": params["email"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    user = ctx.step("create-user-record", create_user_record)

    # Demo only: fail once after the first checkpoint so the retry behavior is visible.
    outage = ctx.begin_step("demo-transient-outage")
    if not outage.done:
        print(f"[{ctx.task_id}] simulating a temporary email provider outage")
        ctx.complete_step(outage, {"simulated": True})
        raise RuntimeError("temporary email provider outage")

    def send_activation_email():
        print(f"[{ctx.task_id}] sending activation email to {user['email']}")
        return {
            "sent": True,
            "provider": "demo-mail",
            "to": user["email"],
        }

    delivery = ctx.step("send-activation-email", send_activation_email)

    print(f"[{ctx.task_id}] waiting for user-activated:{user['user_id']}")
    activation = ctx.await_event(f"user-activated:{user['user_id']}", timeout=3600)

    return {
        "user_id": user["user_id"],
        "email": user["email"],
        "delivery": delivery,
        "status": "active",
        "activated_at": activation["activated_at"],
    }


print("worker listening on queue default")
app.start_worker()
