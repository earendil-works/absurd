def test_queue_management_round_trip(client):
    client.create_queue("main")
    client.create_queue("main")

    assert client.list_queues() == ["main"]

    client.drop_queue("main")
    assert client.list_queues() == []
