def test_schema_version_defaults_to_main(client):
    assert client.get_schema_version() == "main"


def test_schema_version_function_is_replaceable(client):
    client.conn.execute(
        """
        create or replace function absurd.get_schema_version ()
          returns text
          language sql
        as $$
          select '0.1.1'::text;
        $$;
        """
    )
    assert client.get_schema_version() == "0.1.1"
