def test_spawn_claim_complete_flow(client):
    queue = "lifecycle"
    client.create_queue(queue)

    spawn = client.spawn_task(queue, "echo", {"message": "hi"})

    claimed = client.claim_tasks(queue, worker="tester", claim_timeout=120)
    assert len(claimed) == 1
    run = claimed[0]
    assert run["task_id"] == spawn.task_id
    assert run["attempt"] == 1

    client.complete_run(queue, run["run_id"], {"status": "ok"})

    task_row = client.get_task(queue, spawn.task_id)
    assert task_row is not None
    assert task_row["state"] == "completed"
    assert task_row["completed_payload"] == {"status": "ok"}
    assert task_row["last_attempt_run"] == run["run_id"]

    run_row = client.get_run(queue, run["run_id"])
    assert run_row is not None
    assert run_row["state"] == "completed"
    assert run_row["result"] == {"status": "ok"}
