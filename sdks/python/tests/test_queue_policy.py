import asyncio

from absurd_sdk import Absurd, AsyncAbsurd


def test_sync_queue_policy_methods(conn, queue_name):
    queue = queue_name("py_policy")
    client = Absurd(conn, queue_name="default")

    client.create_queue(
        queue,
        storage_mode="partitioned",
        partition_lookahead="35 days",
        partition_lookback="2 days",
        cleanup_ttl_seconds=12345,
        cleanup_limit=77,
        detach_mode="empty",
        detach_min_age="45 days",
    )

    policy = client.get_queue_policy(queue)
    assert policy is not None
    assert policy["queue_name"] == queue
    assert policy["storage_mode"] == "partitioned"
    assert str(policy["partition_lookahead"]) == "35 days, 0:00:00"
    assert str(policy["partition_lookback"]) == "2 days, 0:00:00"
    assert policy["cleanup_ttl_seconds"] == 12345
    assert policy["cleanup_limit"] == 77
    assert policy["detach_mode"] == "empty"
    assert str(policy["detach_min_age"]) == "45 days, 0:00:00"

    client.set_queue_policy(queue, cleanup_ttl_seconds=4321, cleanup_limit=12)

    updated = client.get_queue_policy(queue)
    assert updated is not None
    assert updated["cleanup_ttl_seconds"] == 4321
    assert updated["cleanup_limit"] == 12


def test_async_queue_policy_methods(db_dsn, queue_name):
    queue = queue_name("apy_policy")

    async def run():
        client = AsyncAbsurd(db_dsn, queue_name="default")
        await client.create_queue(
            queue,
            storage_mode="partitioned",
            partition_lookahead="21 days",
            cleanup_ttl_seconds=999,
        )

        policy = await client.get_queue_policy(queue)
        assert policy is not None
        assert policy["storage_mode"] == "partitioned"
        assert str(policy["partition_lookahead"]) == "21 days, 0:00:00"
        assert policy["cleanup_ttl_seconds"] == 999

        await client.set_queue_policy(queue, detach_mode="empty", detach_min_age="10 days")
        updated = await client.get_queue_policy(queue)
        assert updated is not None
        assert updated["detach_mode"] == "empty"
        assert str(updated["detach_min_age"]) == "10 days, 0:00:00"

        await client.close()

    asyncio.run(run())
