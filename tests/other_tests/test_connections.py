import pytest

from src.db.mongodb_client import mongo_client
from src.db.neo4j_client import neo4j_client
from src.db.postgres_client import db
from src.db.redis_client import redis_client


@pytest.mark.asyncio
async def test_postgres_select():
    with db.get_cursor() as cur:
        cur.execute("SELECT 1 AS ok;")
        assert cur.fetchone()["ok"] == 1


def test_mongo_ping():
    assert mongo_client.db.command("ping")["ok"] == 1.0


def test_redis_ping():
    assert redis_client.client.ping() is True


def test_neo4j_ping():
    q = "RETURN 1 AS ok"
    with neo4j_client.driver.session() as session:
        record = session.run(q).single()
        assert record["ok"] == 1
